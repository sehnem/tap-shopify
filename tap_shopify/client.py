"""GraphQL client handling, including ShopifyStream base class."""

from __future__ import annotations

import math
from functools import cached_property
from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Union

import requests  # noqa: TCH002
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import GraphQLStream

from tap_shopify.gql_queries import (
    query_incremental,
    schema_query,
    simple_query,
    simple_query_incremental,
)


class ShopifyStream(GraphQLStream):
    """Shopify stream class."""

    query_name = None
    page_size = 1
    query_cost = None
    available_points = None
    restore_rate = None
    max_points = None
    single_object_params = None
    recursive_objs = []

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        shop = self.config.get("shop")
        api_version = self.config.get("api_version")
        return f"https://{shop}.myshopify.com/admin/api/{api_version}/graphql.json"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["X-Shopify-Access-Token"] = self.config.get("auth_token")
        return headers

    @cached_property
    def schema_gql(self) -> dict:
        """Return the schema for the stream."""
        request_data = {"query": schema_query}
        response = requests.post(
            self.url_base, json=request_data, headers=self.http_headers
        )
        return response.json()["data"]["__schema"]["types"]

    def extract_field_type(self, field) -> str:
        """Extract the field type from the schema."""
        type_mapping = {
            "Boolean": th.BooleanType,
            "DateTime": th.DateTimeType,
            "Float": th.NumberType,
            "Int": th.IntegerType,
        }
        name = field["name"]
        kind = field["kind"]
        if kind == "ENUM":
            field_type = th.StringType
        elif kind == "NON_NULL":
            type_def = field.get("type", field)["ofType"]
            field_type = self.extract_field_type(type_def)
        elif kind == "SCALAR":
            field_type = type_mapping.get(name, th.StringType)
        elif kind == "OBJECT":
            obj_schema = self.extract_gql_schema(name)
            properties = self.get_fields_schema(obj_schema["fields"])
            if not properties:
                return
            field_type = th.ObjectType(*properties)
        elif kind == "LIST":
            obj_type = field["ofType"]["ofType"]
            list_field_type = self.extract_field_type(obj_type)
            field_type = th.ArrayType(list_field_type)
        else:
            return
        return field_type

    def get_fields_schema(self, fields) -> dict:
        """Build the schema for the stream."""
        properties = []
        for field in fields:
            field_name = field["name"]
            # Ignore all the fields that need arguments
            if field.get("args") or field.get("isDeprecated"):
                continue
            if field_name in self.recursive_objs:
                continue
            if field["type"]["kind"] == "INTERFACE":
                continue

            if field["type"]["ofType"]:
                type_def = field.get("type", field)["ofType"]
                field_type = self.extract_field_type(type_def)
                if field_type:
                    property = th.Property(field_name, field_type)
                    properties.append(property)
        return properties

    def extract_gql_schema(self, gql_type):
        """Extract the schema for the stream."""
        gql_type_lw = gql_type.lower()
        schema_gen = (s for s in self.schema_gql if s["name"].lower() == gql_type_lw)
        return next(schema_gen, None)

    @cached_property
    def schema(self) -> dict:
        """Return the schema for the stream."""
        stream_type = self.extract_gql_schema(self.gql_type)
        properties = self.get_fields_schema(stream_type["fields"])
        return th.PropertiesList(*properties).to_dict()

    @cached_property
    def selected_properties(self):
        """Return the selected properties from the schema."""
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties

    @property
    def gql_selected_fields(self):
        """Return the selected fields for the stream."""
        schema = self.schema["properties"]
        catalog = {k: v for k, v in schema.items() if k in self.selected_properties}

        def denest_schema(schema):
            output = ""
            for key, value in schema.items():
                if "items" in value.keys():
                    value = value["items"]
                if "properties" in value.keys():
                    denested = denest_schema(value["properties"])
                    output = f"{output}\n{key}\n{{{denested}\n}}"
                else:
                    output = f"{output}\n{key}"
            return output

        return denest_schema(catalog)

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Prepare the data payload for the GraphQL API request."""
        params = self.get_url_params(context, next_page_token)
        query = self.query.lstrip()
        request_data = {
            "query": query,
            "variables": params,
        }
        self.logger.debug(f"Attempting query:\n{query}")
        return request_data

    @property
    def page_size(self) -> int:
        """Return the page size for the stream."""
        if not self.available_points:
            return 1
        pages = self.available_points / self.query_cost
        if pages < 5:
            points_to_restore = self.max_points - self.available_points
            sleep(points_to_restore // self.restore_rate - 1)
            pages = (self.max_points - self.restore_rate) / self.query_cost
            pages = pages - 1
        elif self.query_cost and pages > 5:
            if self.query_cost * pages >= 1000:
                pages = math.floor(1000 / self.query_cost)
            else:
                pages = 250 if pages > 250 else pages
        return int(pages)

    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        if not self.replication_key and not self.single_object_params:
            base_query = simple_query
        elif self.single_object_params:
            base_query = simple_query_incremental
        else:
            base_query = query_incremental

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)

        return query

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Any:
        """Return token identifying next page or None if all records have been read."""
        if not self.replication_key:
            return None
        response_json = response.json()
        has_next_json_path = f"$.data.{self.query_name}.pageInfo.hasNextPage"
        has_next = next(extract_jsonpath(has_next_json_path, response_json))
        if has_next:
            cursor_json_path = f"$.data.{self.query_name}.edges[-1].cursor"
            all_matches = extract_jsonpath(cursor_json_path, response_json)
            return next(all_matches, None)
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}
        params["first"] = self.page_size
        if next_page_token:
            params["after"] = next_page_token
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                params["filter"] = f"updated_at:>{date}"
        if self.single_object_params:
            params = self.single_object_params
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.replication_key:
            json_path = f"$.data.{self.query_name}.edges[*].node"
        else:
            json_path = f"$.data.{self.query_name}"
        response = response.json()

        if response.get("errors"):
            raise Exception(response["errors"])

        cost = response["extensions"].get("cost")
        if not self.query_cost:
            self.query_cost = cost.get("requestedQueryCost")
        self.available_points = cost["throttleStatus"].get("currentlyAvailable")
        self.restore_rate = cost["throttleStatus"].get("restoreRate")
        self.max_points = cost["throttleStatus"].get("maximumAvailable")

        yield from extract_jsonpath(json_path, input=response)

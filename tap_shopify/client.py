"""GraphQL client handling, including ShopifyStream base class."""

from __future__ import annotations

from functools import cached_property
from inspect import stack
from typing import Any, Optional

import requests  # noqa: TCH002
from singer_sdk import typing as th
from singer_sdk.streams import GraphQLStream

from tap_shopify.gql_queries import schema_query


def verify_recursion(func):
    """Verify if the stream is recursive."""
    objs = []

    def wrapper(*args, **kwargs):
        if not [f for f in stack() if f.function == func.__name__]:
            objs.clear()
        field_name = args[1]["name"]
        field_kind = args[1]["kind"]
        if field_name not in objs:
            if field_kind == "OBJECT":
                objs.append(args[1]["name"])
            result = func(*args, **kwargs)
            return result

    return wrapper


class ShopifyStream(GraphQLStream):
    """Shopify stream class."""

    query_name = None
    page_size = 1
    query_cost = None
    available_points = None
    restore_rate = None
    max_points = None
    single_object_params = None
    ignore_objs = []

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

    @verify_recursion
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
            return th.StringType
        elif kind == "NON_NULL":
            type_def = field.get("type", field)["ofType"]
            return self.extract_field_type(type_def)
        elif kind == "SCALAR":
            return type_mapping.get(name, th.StringType)
        elif kind == "OBJECT":
            obj_schema = self.extract_gql_schema(name)
            properties = self.get_fields_schema(obj_schema["fields"])
            if properties:
                return th.ObjectType(*properties)
        elif kind == "LIST":
            obj_type = field["ofType"]["ofType"]
            list_field_type = self.extract_field_type(obj_type)
            if list_field_type:
                return th.ArrayType(list_field_type)

    def get_fields_schema(self, fields) -> dict:
        """Build the schema for the stream."""
        properties = []
        for field in fields:
            field_name = field["name"]
            # Ignore all the fields that need arguments
            if field.get("args") or field.get("isDeprecated"):
                continue
            if field_name in self.ignore_objs:
                continue
            if field["type"]["kind"] == "INTERFACE":
                continue

            type_def = field.get("type", field)
            type_def = type_def["ofType"] or type_def
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
    def catalog_dict(self):
        """Return the catalog for the stream."""
        if getattr(self._tap, "input_catalog"):
            catalog = self._tap.input_catalog.to_dict()
            return catalog["streams"]
        return {}

    @cached_property
    def schema(self) -> dict:
        """Return the schema for the stream."""
        if getattr(self._tap, "input_catalog"):
            streams = self.catalog_dict
            stream = (s for s in streams if s["tap_stream_id"] == self.name)
            stream_catalog = next(stream, None)
            if stream_catalog:
                return stream_catalog["schema"]
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

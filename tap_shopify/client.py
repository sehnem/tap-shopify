"""GraphQL client handling, including ShopifyStream base class."""

from __future__ import annotations

from functools import cached_property
from inspect import stack
from typing import Any, Optional
import requests

from singer_sdk.exceptions import FatalAPIError, RetriableAPIError

from http import HTTPStatus

from singer_sdk import typing as th
from singer_sdk.pagination import SinglePagePaginator
from singer_sdk.streams import GraphQLStream

from tap_shopify.auth import ShopifyAuthenticator
from tap_shopify.gql_queries import schema_query
from tap_shopify.paginator import ShopifyPaginator


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
    single_object_params = None
    ignore_objs = []
    _requests_session = None
    nested_connections = []

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store = self.config.get("store")
        api_version = self.config.get("api_version")
        return f"https://{store}.myshopify.com/admin/api/{api_version}/graphql.json"

    @property
    def authenticator(self):
        """Return a new authenticator object."""
        return ShopifyAuthenticator(
            self,
            key="X-Shopify-Access-Token",
            value=self.config["access_token"],
            location="header",
        )

    @property
    def get_new_paginator(self):
        if not self.replication_key or self.config.get("bulk"):
            paginator = SinglePagePaginator
        else:
            paginator = ShopifyPaginator
        return paginator

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        headers["Content-Type"] = "application/json"
        return headers

    @cached_property
    def schema_gql(self) -> dict:
        """Return the schema for the stream."""
        return self._tap.schema_gql
    
    @cached_property
    def additional_arguments(self) -> dict:
        """Return the schema for the stream."""
        gql_query = next(q for q in self._tap.queries_gql if q["name"]==self.query_name)
        if "includeClosed" in [a["name"] for a in gql_query["args"]]:
            return ["includeClosed: true"]
        return []

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

        if kind == "OBJECT":
            if name in self._tap.gql_types_in_schema:
                return th.ObjectType(th.Property("id", th.StringType, required=True))
            obj_schema = self.extract_gql_schema(name)
            properties = self.get_fields_schema(obj_schema["fields"])
            if properties:
                return th.ObjectType(*properties)
        elif kind == "LIST":
            obj_type = field["ofType"]["ofType"]
            list_field_type = self.extract_field_type(obj_type)
            if list_field_type:
                return th.ArrayType(list_field_type)
        elif kind == "ENUM":
            return th.StringType
        elif kind == "NON_NULL":
            type_def = field.get("type", field)["ofType"]
            return self.extract_field_type(type_def)
        elif kind == "SCALAR":
            return type_mapping.get(name, th.StringType)

    def get_fields_schema(self, fields) -> dict:
        """Build the schema for the stream."""
        properties = []
        for field in fields:
            field_name = field["name"]
            # Ignore all the fields that need arguments
            if field.get("isDeprecated") and self.config.get("ignore_deprecated"):
                continue
            if field.get("args"):
                if field["args"][0]["name"] == "first":
                    self.nested_connections.append(field_name)
                continue
            if field_name in self.ignore_objs:
                continue
            if field["type"]["kind"] == "INTERFACE":
                continue

            required = field["type"].get("kind") == "NON_NULL"
            type_def = field.get("type", field)
            type_def = type_def["ofType"] or type_def
            field_type = self.extract_field_type(type_def)
            if field_type:
                property = th.Property(field_name, field_type, required=required)
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
            if isinstance(key, tuple) and len(key) == 2:
                field_name = key[-1]
                if (
                    value.selected
                    or value.selected_by_default
                    or field_name in self.primary_keys
                    or field_name == self.replication_key
                ):
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


    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""

        if (
            response.status_code in self.extra_retry_statuses
            or HTTPStatus.INTERNAL_SERVER_ERROR
            <= response.status_code
            <= max(HTTPStatus)
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)
        
        json_resp = response.json()

        if errors:=json_resp.get("errors"):
            if len(errors)==1:
                error = errors[0]
                code = error.get("extensions", {}).get("code")
                if code in ["THROTTLED", "MAX_COST_EXCEEDED"]:
                    raise RetriableAPIError(error.get("message", ""), response)
                raise FatalAPIError(error.get("message", ""))
            raise RetriableAPIError(json_resp["errors"], response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)

    def convert_id_fields(self, row: dict) -> dict:
        """Convert the id fields to string."""
        if not isinstance(row, dict):
            return row
        for key, value in row.items():
            if key=="id" and isinstance(value, str):
                row["id"] = row["id"].split("/")[-1].split("?")[0]
            elif isinstance(value, dict):
                row[key] = self.convert_id_fields(value)
            elif isinstance(value, list):
                row[key] = [self.convert_id_fields(v) for v in value]
        return row

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure."""

        if self.config["use_numeric_ids"]:
            self.convert_id_fields(row)

        return row
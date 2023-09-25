"""GraphQL client handling, including ShopifyStream base class."""

from __future__ import annotations

from datetime import datetime
from functools import cached_property
from http import HTTPStatus
from inspect import stack
from time import sleep
from typing import Any, Dict, Iterable, Optional, cast

import requests
import simplejson
from singer_sdk import typing as th
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import SinglePagePaginator
from singer_sdk.streams import GraphQLStream
from memoization import cached

from tap_shopify.exceptions import InvalidOperation, OperationFailed
from tap_shopify.gql_queries import (
    bulk_query,
    bulk_query_status,
    query_incremental,
)
from tap_shopify.paginator import ShopifyPaginator


def verify_recursion(func):
    """Verify if the stream is recursive."""
    objs = []
    connections = dict(num=0, in_conn=False)

    def wrapper(*args, **kwargs):
        if not [f for f in stack() if f.function == func.__name__]:
            connections["in_conn"] = False
            objs.clear()

        field_name = args[1]["name"]
        field_kind = args[1]["kind"]

        if field_kind == "INTERFACE":
            if connections["in_conn"] or connections["num"] >= 5:
                return
            connections["in_conn"] = True
            connections["num"] += 1

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
    ignore_objs = [
        "image",
        "metafield",
        "metafields",
        "metafieldconnection",
        "privateMetafield",
        "privateMetafields",
    ]
    _requests_session = None
    denied_fields = []
    stream_connections = []

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        store = self.config.get("store")
        api_version = self.config.get("api_version")
        return f"https://{store}.myshopify.com/admin/api/{api_version}/graphql.json"

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
        headers["X-Shopify-Access-Token"] = self.config["access_token"]
        return headers

    @cached_property
    def schema_gql(self) -> dict:
        """Return the schema for the stream."""
        return self._tap.schema_gql

    @cached_property
    def additional_arguments(self) -> dict:
        """Return the schema for the stream."""
        gql_query = next(
            q for q in self._tap.queries_gql if q["name"] == self.query_name
        )
        if "includeClosed" in [a["name"] for a in gql_query["args"]]:
            return ["includeClosed: true"]
        return []

    @cached
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
            if not obj_type:
                return None
            list_field_type = self.extract_field_type(obj_type)
            if list_field_type:
                if (
                    obj_type["name"].endswith("Edge")
                    and not "node" in list_field_type.type_dict["properties"].keys()
                ):
                    return None
                return th.ArrayType(list_field_type)
        elif kind == "INTERFACE" and self.config.get("bulk"):
            obj_schema = self.extract_gql_schema(name)
            properties = self.get_fields_schema(obj_schema["fields"])
        elif kind == "ENUM":
            return th.StringType
        elif kind == "NON_NULL":
            type_def = field.get("type", field)["ofType"]
            return self.extract_field_type(type_def)
        elif kind == "SCALAR":
            return type_mapping.get(name, th.StringType)

    def extract_gql_schema(self, gql_type):
        """Extract the schema for the stream."""
        gql_type_lw = gql_type.lower()
        schema_gen = (s for s in self.schema_gql if s["name"].lower() == gql_type_lw)
        return next(schema_gen, None)

    def get_fields_schema(self, fields) -> dict:
        """Build the schema for the stream."""
        # Filtering the fields that are not needed
        field_names = [f["name"] for f in fields]
        if "edges" in field_names:
            fields = [f for f in fields if f["name"] == "edges"]
        elif "node" in field_names:
            fields = [f for f in fields if f["name"] == "node"]

        properties = []
        for field in fields:
            field_name = field["name"]
            type_def = field.get("type", field)
            type_def = type_def["ofType"] or type_def
            # Ignore all the fields that need arguments
            if field.get("isDeprecated") and self.config.get("ignore_deprecated"):
                continue
            if field_name in self.ignore_objs:
                continue

            if type_def.get("name") and type_def["name"].endswith("Connection"):
                self.stream_connections.append(
                    dict(name=field_name, of_type=type_def["name"][:-10])
                )

            required = field["type"].get("kind") == "NON_NULL"
            field_type = self.extract_field_type(type_def)

            if field_type:
                property = th.Property(field_name, field_type, required=required)
                properties.append(property)
        return properties

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
                metadata = next(
                    f for f in stream_catalog["metadata"] if not f["breadcrumb"]
                )
                if not metadata["metadata"].get("selected"):
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
                if key in self.denied_fields:
                    continue
                if "items" in value.keys():
                    value = value["items"]
                if "properties" in value.keys():
                    denested = denest_schema(value["properties"])
                    output = f"{output}\n{key}\n{{{denested}\n}}"
                else:
                    output = f"{output}\n{key}"
            return output

        return denest_schema(catalog)

    @cached_property
    def selected_connections(self):
        """Return the selected connections for the stream."""
        return [
            c for c in self.stream_connections if c["name"] in self.selected_properties
        ]

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

        if errors := json_resp.get("errors"):
            if len(errors) == 1:
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
            if key == "id" and isinstance(value, str):
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

    def query_gql(self) -> str:
        """Set or return the GraphQL query string."""
        base_query = query_incremental

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)
        additional_args = ", " + ", ".join(self.additional_arguments)
        query = query.replace("__additional_args__", additional_args)

        if self.selected_connections:
            conn_names = list(set([c["name"] for c in self.selected_connections]))
            for conn_name in conn_names:
                query = query.replace(conn_name, f"{conn_name}(first:1)")
        return query

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = {}

        if next_page_token:
            params.update(next_page_token)
        else:
            params["first"] = 1
        if self.replication_key:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                params["filter"] = f"updated_at:>{date}"
        if self.single_object_params:
            params = self.single_object_params
        return params

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

    def parse_response_gql(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.replication_key:
            json_path = f"$.data.{self.query_name}.edges[*].node"
        else:
            json_path = f"$.data.{self.query_name}"
        json_resp = response.json()

        yield from extract_jsonpath(json_path, json_resp)

    def query_bulk(self) -> str:
        """Set or return the GraphQL query string."""
        base_query = bulk_query

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)
        filters = f"({self.filters})" if self.filters else ""
        query = query.replace("__filters__", filters)

        return query

    @property
    def filters(self):
        """Return a dictionary of values to be used in URL parameterization."""
        filters = []
        if self.additional_arguments:
            filters.extend(self.additional_arguments)
        if self.replication_key:
            start_date = self.get_starting_timestamp({})
            if start_date:
                date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
                filters.append(f'query: "updated_at:>{date}"')
        return ",".join(filters)

    def get_operation_status(self):
        headers = self.http_headers
        authenticator = self.authenticator
        if authenticator:
            headers.update(authenticator.auth_headers or {})

        request = cast(
            requests.PreparedRequest,
            self.requests_session.prepare_request(
                requests.Request(
                    method=self.rest_method,
                    url=self.get_url({}),
                    headers=headers,
                    json=dict(query=bulk_query_status, variables={}),
                ),
            ),
        )

        decorated_request = self.request_decorator(self._request)
        response = decorated_request(request, {})

        return response

    def check_status(self, operation_id, sleep_time=10, timeout=36000):
        status_jsonpath = "$.data.currentBulkOperation"
        start = datetime.now().timestamp()

        while datetime.now().timestamp() < (start + timeout):
            status_response = self.get_operation_status()
            status = next(
                extract_jsonpath(status_jsonpath, input=status_response.json())
            )
            if status["id"] != operation_id:
                raise InvalidOperation(
                    "The current job was not triggered by the process, "
                    "check if other service is using the Bulk API"
                )
            if status["url"]:
                return status["url"]
            if status["status"] == "FAILED":
                raise InvalidOperation(f"Job failed: {status['errorCode']}")
            self.logger.info(f"Bulk request with status {status.get('status')} - Object count: {status.get('objectCount')}")
            sleep(sleep_time)
        raise OperationFailed("Job Timeout")

    def parse_response_bulk(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        operation_id_jsonpath = "$.data.bulkOperationRunQuery.bulkOperation.id"
        error_jsonpath = "$.data.bulkOperationRunQuery.userErrors"
        json_resp = response.json()
        errors = next(extract_jsonpath(error_jsonpath, json_resp), None)
        if errors:
            raise InvalidOperation(simplejson.dumps(errors))
        operation_id = next(extract_jsonpath(operation_id_jsonpath, json_resp))

        url = self.check_status(operation_id)

        output = requests.get(url, stream=True, timeout=30)
        main_item = None
        for line in output.iter_lines():
            line = simplejson.loads(line)
            if "__parentId" not in line.keys():
                if main_item:
                    yield main_item
                main_item = line
                for sc in self.selected_connections:
                    main_item[sc["name"]] = {}
                    main_item[sc["name"]]["edges"] = []
                main_item["variants"] = {}
                main_item["variants"]["edges"] = []
            elif main_item["id"] == line["__parentId"]:
                del line["__parentId"]
                line_type = line["id"].split("/")[-2]
                field_name = next(
                    c["name"] for c in self.selected_connections if c["of_type"] == line_type
                )
                main_item[field_name]["edges"].append(dict(node=line))
            else:
                pass

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.config.get("bulk"):
            return self.parse_response_bulk(response)
        return self.parse_response_gql(response)

    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        # TODO: figure out how to handle interfaces
        self.evaluate_query()
        if self.config.get("bulk"):
            return self.query_bulk()
        return self.query_gql()

    def evaluate_query(self) -> dict:
        query = self.query_gql().lstrip()
        params = self.get_url_params(None, None)
        request_data = {
            "query": query,
            "variables": params,
        }

        response = requests.request(
            method=self.rest_method,
            url=self.get_url({}),
            params=params,
            headers=self.http_headers,
            json=request_data,
        )

        errors = response.json().get("errors")
        if errors:
            for error in errors:
                error_code = error.get("extensions", {}).get("code")
                if error_code in ["ACCESS_DENIED"]:
                    message = error.get("message", "")
                    if message.startswith("Access denied for "):
                        self.logger.warning(message)
                        self.denied_fields.append(message.split(" ")[3])
                else:
                    raise FatalAPIError(error.get("message", ""), response)
            self.evaluate_query()

    # TODO: get query cost from here

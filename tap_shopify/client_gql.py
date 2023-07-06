"""GraphQL client handling, including shopify-betaStream base class."""

from __future__ import annotations

import math
from time import sleep
from typing import Any, Dict, Iterable, Optional

import requests  # noqa: TCH002
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_shopify.client import ShopifyStream
from tap_shopify.gql_queries import query_incremental


class shopifyGqlStream(ShopifyStream):
    """shopify stream class."""

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

    # @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""

        # This is for supporting the single object like shop endpoint
        # if not self.replication_key and not self.single_object_params:
        #     base_query = simple_query
        # elif self.single_object_params:
        #     base_query = simple_query_incremental
        # else:
        #     base_query = query_incremental

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

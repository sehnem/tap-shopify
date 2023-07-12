"""GraphQL client handling, including shopify-betaStream base class."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import requests  # noqa: TCH002
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_shopify.client import ShopifyStream
from tap_shopify.gql_queries import query_incremental


class shopifyGqlStream(ShopifyStream):
    """shopify stream class."""

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

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.replication_key:
            json_path = f"$.data.{self.query_name}.edges[*].node"
        else:
            json_path = f"$.data.{self.query_name}"
        response = response.json()

        if response.get("errors"):
            raise Exception(response["errors"])

        yield from extract_jsonpath(json_path, input=response)

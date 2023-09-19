"""GraphQL client handling, including shopify-betaStream base class."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

import requests  # noqa: TCH002
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_shopify.client import ShopifyStream
from tap_shopify.gql_queries import query_incremental


class shopifyGqlStream(ShopifyStream):
    """shopify stream class."""

    def query_gql(self) -> str:
        """Set or return the GraphQL query string."""
        base_query = query_incremental

        query = base_query.replace("__query_name__", self.query_name)
        query = query.replace("__selected_fields__", self.gql_selected_fields)
        additional_args = ", " + ", ".join(self.additional_arguments)
        query = query.replace("__additional_args__", additional_args)

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

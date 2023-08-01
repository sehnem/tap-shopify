import math
from functools import cached_property
from time import sleep

import requests
from requests import Response
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator


class ShopifyPaginator(BaseAPIPaginator):
    """shopify paginator class."""

    query_cost = None
    available_points = None
    restore_rate = None
    max_points = None

    def __init__(self) -> None:
        """Create a new paginator.

        Args:
            start_value: Initial value.
        """
        self._value = None
        self._page_count = 0
        self._finished = False
        self._last_seen_record: dict | None = None

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

    def query_name(self, response_json) -> str:
        """Set or return the GraphQL query name."""
        return list(response_json.get("data"))[0]

    def get_next(self, response: requests.Response):
        """Get the next pagination value."""

        response_json = response.json()
        query_name = self.query_name(response_json)

        cost = response_json["extensions"].get("cost")

        self.query_cost = cost.get("requestedQueryCost")
        self.available_points = cost["throttleStatus"].get("currentlyAvailable")
        self.restore_rate = cost["throttleStatus"].get("restoreRate")
        self.max_points = cost["throttleStatus"].get("maximumAvailable")

        has_next_json_path = f"$.data.{query_name}.pageInfo.hasNextPage"
        has_next = next(extract_jsonpath(has_next_json_path, response_json))

        if has_next:
            cursor_json_path = f"$.data.{query_name}.edges[-1].cursor"
            all_matches = extract_jsonpath(cursor_json_path, response_json)
            return next(all_matches, None)

        return None

    @property
    def current_value(self):
        """Get the current pagination value.

        Returns:
            Current page value.
        """
        return dict(first=self.page_size, after=self._value)

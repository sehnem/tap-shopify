"""GraphQL client handling, including shopify-betaStream base class."""

from datetime import datetime
from time import sleep
from typing import Any, Iterable, Optional, cast

import requests
import simplejson
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import SinglePagePaginator

from tap_shopify.client import ShopifyStream
from tap_shopify.exceptions import InvalidOperation, OperationFailed
from tap_shopify.gql_queries import bulk_query, bulk_query_status, simple_query


class shopifyBulkStream(ShopifyStream):
    """shopify stream class."""
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        if self.name == "shop":
            base_query = simple_query
        else:
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

    def check_status(self, operation_id, sleep_time=10, timeout=1800):
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
            sleep(sleep_time)
        raise OperationFailed("Job Timeout")

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        operation_id_jsonpath = "$.data.bulkOperationRunQuery.bulkOperation.id"
        error_jsonpath = "$.data.bulkOperationRunQuery.userErrors"
        json_resp = response.json()
        errors = next(extract_jsonpath(error_jsonpath, json_resp), None)
        if errors:
            raise InvalidOperation(simplejson.dumps(errors))
        operation_id = next(
            extract_jsonpath(operation_id_jsonpath, json_resp)
        )

        url = self.check_status(operation_id)

        output = requests.get(url, stream=True, timeout=30)

        for line in output.iter_lines():
            yield simplejson.loads(line)

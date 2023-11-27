"""Script to generate the graphql schema for the Shopify API."""

import json
import requests

API_VERSIONS = ["2022-10", "2023-01", "2023-04", "2023-07", "2023-10"]

with open("tap-shopify/.secrets/config.json") as f:
    configs = json.load(f)

store = configs["store"]
access_token = configs["access_token"]

headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": access_token,
}

with open("tap-shopify/tap_shopify/graphql_queries/schema.gql") as f:
    schema_gql = f.read()

with open("tap-shopify/tap_shopify/graphql_queries/queries.gql") as f:
    queries_gql = f.read()


def request_gql(store: str, api_version: str, query: str) -> requests.Response:
    url = f"https://{store}.myshopify.com/admin/api/{api_version}/graphql.json"
    resp = requests.post(
        url=url,
        headers=headers,
        json=dict(query=query),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


for api_version in API_VERSIONS:
    schema_response = request_gql(store, api_version, schema_gql)
    with open(f"tap-shopify/tap_shopify/graphql_schemas/{api_version}_types.json", "w") as f:
        json.dump(schema_response, f)

    queries_response = request_gql(store, api_version, queries_gql)
    with open(f"tap-shopify/tap_shopify/graphql_schemas/{api_version}_queries.json", "w") as f:
        json.dump(queries_response, f)

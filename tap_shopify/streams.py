"""Stream type classes for tap-shopify."""

from __future__ import annotations

from functools import cached_property
from typing import Any, Iterable, Optional

import requests  # noqa: TCH002

from tap_shopify.client_bulk import shopifyBulkStream
from tap_shopify.client_gql import shopifyGqlStream


class ShopifyStream(shopifyGqlStream, shopifyBulkStream):
    """Define base based on the type GraphQL or Bulk."""

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        if self.config.get("bulk"):
            return shopifyBulkStream.parse_response(self, response)
        else:
            return shopifyGqlStream.parse_response(self, response)

    @cached_property
    def query(self) -> str:
        """Set or return the GraphQL query string."""
        if self.config.get("bulk"):
            return shopifyBulkStream.query(self)
        else:
            return shopifyGqlStream.query(self)


class CustomCollectionsStream(ShopifyStream):
    """Custom collections stream."""

    name = "collections"
    gql_type = "Collection"
    query_name = "collections"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class CustomersStream(ShopifyStream):
    """Define customers stream."""

    name = "customers"
    gql_type = "Customer"
    query_name = "customers"
    ignore_objs = ["customer", "paymentCollectionDetails"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class FulfillmentOrdersStream(ShopifyStream):
    """Define fulfillment orders stream."""

    name = "fulfillment_orders"
    gql_type = "FulfillmentOrder"
    query_name = "fulfillmentOrders"
    ignore_objs = ["paymentCollectionDetails"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class InventoryItemsStream(ShopifyStream):
    """Define inventory items stream."""

    name = "inventory_items"
    gql_type = "InventoryItem"
    query_name = "inventoryItems"
    ignore_objs = ["inventoryItem"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class OrdersStream(ShopifyStream):
    """Define orders lists stream."""

    name = "orders"
    gql_type = "Order"
    query_name = "orders"
    ignore_objs = ["paymentCollectionDetails"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class ProductsStream(ShopifyStream):
    """Define products stream."""

    name = "products"
    gql_type = "Product"
    query_name = "products"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class VariantsStream(ShopifyStream):
    """Define variants stream."""

    name = "variants"
    gql_type = "ProductVariant"
    query_name = "productVariants"
    primary_keys = ["id"]
    ignore_objs = ["variant"]
    replication_key = "updatedAt"

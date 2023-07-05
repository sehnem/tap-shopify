"""Stream type classes for tap-shopify."""

from __future__ import annotations

from tap_shopify.client import ShopifyStream


class CustomersStream(ShopifyStream):
    """Define customers stream."""

    name = "customers"
    gql_type = "Customer"
    query_name = "customers"
    recursive_objs = ["customer"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class InventoryItemsStream(ShopifyStream):
    """Define inventory items stream."""

    name = "inventory_items"
    gql_type = "InventoryItem"
    query_name = "inventoryItems"
    recursive_objs = ["inventoryItem"]
    primary_keys = ["id"]
    replication_key = "updatedAt"


class OrdersStream(ShopifyStream):
    """Define orders lists stream."""

    name = "orders"
    gql_type = "Order"
    query_name = "orders"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class FulfillmentOrdersStream(ShopifyStream):
    """Define fulfillment orders stream."""

    name = "fulfillment_orders"
    gql_type = "FulfillmentOrder"
    query_name = "fulfillmentOrders"
    primary_keys = ["id"]
    replication_key = "updatedAt"


class PriceListsStream(ShopifyStream):
    """Define price lists stream."""

    name = "price_lists"
    gql_type = "PriceList"
    query_name = "priceLists"
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
    recursive_objs = ["variant"]
    replication_key = "updatedAt"

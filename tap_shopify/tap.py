"""Shopify tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_shopify import streams


class TapShopify(Tap):
    """Shopify tap class."""

    name = "tap-shopify"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,
            description="The token to authenticate against the API service.",
        ),
        th.Property(
            "store",
            th.StringType,
            required=True,
            description="The shopify shop name.",
        ),
        th.Property(
            "api_version",
            th.StringType,
            default="2023-04",
            description="The version of the API to use.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync.",
        ),
        th.Property(
            "bulk",
            th.BooleanType,
            default=False,
            description="To use the bulk API or not.",
        ),
        th.Property(
            "ignore_deprecated",
            th.BooleanType,
            default=True,
            description="To ignore deprecated fields or not.",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.ShopifyStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CustomersStream(self),
            streams.FulfillmentOrdersStream(self),
            streams.InventoryItemsStream(self),
            streams.OrdersStream(self),
            streams.ProductsStream(self),
            streams.VariantsStream(self),
        ]


if __name__ == "__main__":
    TapShopify.cli()

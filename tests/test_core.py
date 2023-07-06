"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_standard_tap_tests

from tap_shopify.tap import TapShopify

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "auth_token": "shpat_ffffffffffffffffffffffffffffffff",
    "shop": "teststore",
    "bulk": False
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    _ = get_standard_tap_tests(TapShopify, config=SAMPLE_CONFIG)
"""Shopify Authentication."""

from singer_sdk.authenticators import APIKeyAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes the streams reuse the same authenticator instance.
class ShopifyAuthenticator(APIKeyAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for tap_shopify."""
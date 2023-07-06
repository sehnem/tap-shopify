"""Exceptions for tap-shopify."""

import requests


class InvalidOperation(requests.RequestException, ValueError):
    """Invalid job id."""


class OperationFailed(requests.RequestException, ValueError):
    """Operation Failed."""

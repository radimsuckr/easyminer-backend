import pytest
from fastapi import HTTPException

from easyminer.api.security import get_api_key


def test_api_key_from_header_with_prefix():
    api_key_h = "ApiKey test-key-123"
    api_key_q = None

    result = get_api_key(api_key_h, api_key_q)

    assert result == "test-key-123"


def test_api_key_from_query_parameter():
    api_key_h = None
    api_key_q = "test-key-456"

    result = get_api_key(api_key_h, api_key_q)

    assert result == "test-key-456"


def test_api_key_header_without_prefix_fails():
    api_key_h = "test-key-789"
    api_key_q = None

    with pytest.raises(HTTPException) as exc_info:
        _ = get_api_key(api_key_h, api_key_q)

    assert exc_info.value.status_code == 401
    assert "ApiKey" in exc_info.value.detail


def test_api_key_header_prioritized_over_query():
    api_key_h = "ApiKey header-key"
    api_key_q = "query-key"

    result = get_api_key(api_key_h, api_key_q)

    assert result == "header-key"


def test_missing_api_key_fails():
    api_key_h = None
    api_key_q = None

    with pytest.raises(HTTPException) as exc_info:
        _ = get_api_key(api_key_h, api_key_q)

    assert exc_info.value.status_code == 401
    assert "Missing API key" in exc_info.value.detail


def test_api_key_with_bearer_prefix_fails():
    api_key_h = "Bearer test-key-123"
    api_key_q = None

    with pytest.raises(HTTPException) as exc_info:
        _ = get_api_key(api_key_h, api_key_q)

    assert exc_info.value.status_code == 401
    assert "ApiKey" in exc_info.value.detail

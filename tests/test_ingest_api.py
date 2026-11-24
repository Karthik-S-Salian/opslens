import re

from opslens.api.ingest import UPLOAD_KEY, safe_object_key


def test_safe_object_key_strips_paths_and_has_matching_shape():
    key = safe_object_key("../../etc/passwd")

    assert key.startswith("uploads/")
    assert key.endswith("/passwd")


def test_safe_object_key_preserves_extension():
    key = safe_object_key("gateway-2026-11-24.log.gz")
    assert key.endswith("/gateway-2026-11-24.log.gz")


def test_safe_object_key_is_unique():
    assert safe_object_key("a.log") != safe_object_key("a.log")


def test_upload_key_rejects_traversal():
    key = safe_object_key("a.log")
    assert UPLOAD_KEY.match(key)
    assert re.match(r"^uploads/[0-9a-f-]{36}/", key)
    assert not UPLOAD_KEY.match("uploads/../../secret")
    assert not UPLOAD_KEY.match("other/123/a.log")

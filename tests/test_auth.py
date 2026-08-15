import json
import time
import pytest
from fastapi.testclient import TestClient
from src.main import app
from src import auth

client = TestClient(app)


def test_auth_disabled_by_default(monkeypatch):
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_ID", "")
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_SECRET", "")
    assert not auth.is_auth_enabled()


def test_session_cookie_signature_roundtrip(monkeypatch):
    monkeypatch.setattr(auth, "AUTH_SECRET_KEY", "test-secret-key-12345")
    user_info = {
        "email": "user@example.com",
        "name": "Test User",
        "picture": "https://example.com/pic.jpg",
        "hd": "example.com",
    }
    cookie_val = auth.create_session_cookie_value(user_info)
    unpacked = auth._verify_and_unpack(cookie_val)
    assert unpacked is not None
    assert unpacked["email"] == "user@example.com"
    assert unpacked["name"] == "Test User"


def test_tampered_cookie_rejected(monkeypatch):
    monkeypatch.setattr(auth, "AUTH_SECRET_KEY", "test-secret-key-12345")
    cookie_val = auth.create_session_cookie_value({"email": "admin@example.com"})
    payload_b64, sig = cookie_val.rsplit(".", 1)
    tampered = payload_b64 + "X." + sig
    assert auth._verify_and_unpack(tampered) is None


def test_expired_cookie_rejected(monkeypatch):
    monkeypatch.setattr(auth, "AUTH_SECRET_KEY", "test-secret-key-12345")
    # Manually construct expired payload
    payload = {
        "email": "user@example.com",
        "exp": int(time.time() - 100),
    }
    raw = auth._sign_data(auth.base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8"))
    assert auth._verify_and_unpack(raw) is None


def test_is_user_authorized_domain_filter(monkeypatch):
    monkeypatch.setattr(auth, "ALLOWED_DOMAINS", {"example.com"})
    monkeypatch.setattr(auth, "ALLOWED_USERS", set())
    
    assert auth.is_user_authorized("admin@example.com", "example.com")
    assert not auth.is_user_authorized("attacker@gmail.com", "gmail.com")


def test_is_user_authorized_specific_user(monkeypatch):
    monkeypatch.setattr(auth, "ALLOWED_DOMAINS", set())
    monkeypatch.setattr(auth, "ALLOWED_USERS", {"vip@special.org"})
    
    assert auth.is_user_authorized("vip@special.org")
    assert not auth.is_user_authorized("other@special.org")


def test_unauthenticated_requests_redirect_or_401(monkeypatch):
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_ID", "dummy-client-id.apps.googleusercontent.com")
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_SECRET", "dummy-secret")
    
    # HTML request -> 302 Redirect to /auth/login
    resp = client.get("/", headers={"accept": "text/html"}, follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"] == "/auth/login"

    # API request -> 401 Unauthorized
    resp_api = client.get("/api/cache/status", headers={"accept": "application/json"})
    assert resp_api.status_code == 401
    assert "Authentication required" in resp_api.json()["detail"]


def test_authenticated_cookie_allows_request(monkeypatch):
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_ID", "dummy-client-id.apps.googleusercontent.com")
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_SECRET", "dummy-secret")
    monkeypatch.setattr(auth, "ALLOWED_DOMAINS", {"example.com"})
    monkeypatch.setattr(auth, "ALLOWED_USERS", {"user@example.com"})
    
    cookie_val = auth.create_session_cookie_value({"email": "user@example.com", "hd": "example.com"})
    client.cookies.set(auth.AUTH_SESSION_COOKIE, cookie_val)
    
    # /auth/me returns user details
    resp = client.get("/auth/me")
    assert resp.status_code == 200
    assert resp.json()["authenticated"] is True
    assert resp.json()["email"] == "user@example.com"
    
    client.cookies.clear()


def test_default_deny_when_no_allowlists_configured(monkeypatch):
    monkeypatch.setattr(auth, "ALLOWED_DOMAINS", set())
    monkeypatch.setattr(auth, "ALLOWED_USERS", set())
    assert not auth.is_user_authorized("anyone@gmail.com", "gmail.com")
    assert not auth.is_user_authorized("anyone@corp.com", "corp.com")


def test_callback_xss_sanitization(monkeypatch):
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_ID", "dummy-client-id")
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_SECRET", "dummy-secret")
    
    xss_payload = "<script>alert(1)</script>"
    resp = client.get(f"/auth/callback?error={xss_payload}")
    assert resp.status_code == 400
    assert "<script>" not in resp.text
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in resp.text


def test_docs_and_openapi_require_auth(monkeypatch):
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_ID", "dummy-client-id")
    monkeypatch.setattr(auth, "GOOGLE_CLIENT_SECRET", "dummy-secret")
    
    resp_docs = client.get("/docs", headers={"accept": "text/html"}, follow_redirects=False)
    assert resp_docs.status_code == 302
    assert resp_docs.headers["location"] == "/auth/login"
    
    resp_openapi = client.get("/openapi.json", headers={"accept": "application/json"})
    assert resp_openapi.status_code == 401

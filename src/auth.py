"""Google OAuth2 authentication module for BigQuery FinOps Optimizer.

Provides browser-based 'Sign in with Google' authentication and session management
without requiring an external Google Cloud Load Balancer or IAP.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Set

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
AUTH_SECRET_KEY = os.environ.get("AUTH_SECRET_KEY", "").strip() or secrets.token_hex(32)
AUTH_SESSION_COOKIE = "finops_session"
AUTH_STATE_COOKIE = "finops_oauth_state"
SESSION_MAX_AGE_SECONDS = int(os.environ.get("AUTH_SESSION_MAX_AGE", str(86400 * 7)))  # 7 days

# Comma-separated allowed hosted domains (e.g. "example.com,corp.internal")
_ALLOWED_DOMAINS_RAW = os.environ.get("ALLOWED_DOMAINS", "").strip()
ALLOWED_DOMAINS: Set[str] = {d.strip().lower() for d in _ALLOWED_DOMAINS_RAW.split(",") if d.strip()}

# Comma-separated allowed specific email addresses (e.g. "admin@example.com")
_ALLOWED_USERS_RAW = os.environ.get("ALLOWED_USERS", "").strip()
ALLOWED_USERS: Set[str] = {u.strip().lower() for u in _ALLOWED_USERS_RAW.split(",") if u.strip()}

_google_request_adapter = google_requests.Request()


def is_auth_enabled() -> bool:
    """Return True if Google OAuth credentials are configured."""
    return bool(GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET)


# ---------------------------------------------------------------------------
# HMAC-SHA256 Signed Session Cookies
# ---------------------------------------------------------------------------

def _sign_data(payload_str: str) -> str:
    sig = hmac.new(AUTH_SECRET_KEY.encode("utf-8"), payload_str.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_str}.{sig}"


def _verify_and_unpack(cookie_value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not cookie_value or "." not in cookie_value:
        return None
    try:
        payload_b64, sig = cookie_value.rsplit(".", 1)
        expected_sig = hmac.new(AUTH_SECRET_KEY.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
        data = json.loads(payload_json)
        if data.get("exp", 0) <= time.time():
            return None
        return data
    except Exception:
        return None


def create_session_cookie_value(user_info: Dict[str, Any]) -> str:
    payload = {
        "email": user_info.get("email", "").lower(),
        "name": user_info.get("name", ""),
        "picture": user_info.get("picture", ""),
        "hd": user_info.get("hd", ""),
        "iat": int(time.time()),
        "exp": int(time.time() + SESSION_MAX_AGE_SECONDS),
    }
    payload_json = json.dumps(payload, separators=(",", ":"))
    payload_b64 = base64.urlsafe_b64encode(payload_json.encode("utf-8")).decode("utf-8")
    return _sign_data(payload_b64)


# ---------------------------------------------------------------------------
# Token Verification & User Authorization
# ---------------------------------------------------------------------------

def verify_google_id_token(token_str: str) -> Optional[Dict[str, Any]]:
    try:
        id_info = google_id_token.verify_oauth2_token(
            token_str, _google_request_adapter, GOOGLE_CLIENT_ID
        )
        return id_info
    except Exception as e:
        logger.debug("Google ID token verification failed: %s", e)
        return None


def is_user_authorized(email: str, hosted_domain: Optional[str] = None) -> bool:
    """Check if the user email or hosted domain matches configured allowlists."""
    email_clean = (email or "").strip().lower()
    domain = hosted_domain or (email_clean.split("@")[-1] if "@" in email_clean else "")

    # If no allowlist is configured, any authenticated Google account is permitted
    if not ALLOWED_DOMAINS and not ALLOWED_USERS:
        return True

    if email_clean in ALLOWED_USERS:
        return True

    if domain and domain.lower() in ALLOWED_DOMAINS:
        return True

    return False


def get_current_user_from_request(request: Request) -> Optional[Dict[str, Any]]:
    # 1. Check signed session cookie
    session_cookie = request.cookies.get(AUTH_SESSION_COOKIE)
    if session_cookie:
        user_data = _verify_and_unpack(session_cookie)
        if user_data and is_user_authorized(user_data.get("email", ""), user_data.get("hd")):
            return user_data

    # 2. Check Bearer Authorization header (for service-to-service / CLI requests)
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header.split("Bearer ", 1)[1].strip()
        id_info = verify_google_id_token(token)
        if id_info and is_user_authorized(id_info.get("email", ""), id_info.get("hd")):
            return id_info

    return None


# ---------------------------------------------------------------------------
# FastAPI Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/auth", tags=["auth"])


def _get_redirect_uri(request: Request) -> str:
    # Use forwarded proto if behind Cloud Run proxy
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", request.url.netloc))
    return f"{proto}://{host}/auth/callback"


@router.get("/login")
def login(request: Request):
    if not is_auth_enabled():
        return RedirectResponse(url="/")

    # If already authenticated, redirect to home
    if get_current_user_from_request(request):
        return RedirectResponse(url="/")

    state = secrets.token_urlsafe(32)
    redirect_uri = _get_redirect_uri(request)

    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "response_type": "code",
        "scope": "openid email profile",
        "redirect_uri": redirect_uri,
        "state": state,
        "prompt": "select_account",
    }
    if ALLOWED_DOMAINS and len(ALLOWED_DOMAINS) == 1:
        params["hd"] = list(ALLOWED_DOMAINS)[0]

    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    response = RedirectResponse(url=auth_url, status_code=302)
    response.set_cookie(
        AUTH_STATE_COOKIE,
        _sign_data(state),
        max_age=600,
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https",
    )
    return response


@router.get("/callback")
def callback(request: Request, code: Optional[str] = None, state: Optional[str] = None, error: Optional[str] = None):
    if not is_auth_enabled():
        return RedirectResponse(url="/")

    if error:
        return HTMLResponse(content=f"<h3>Authentication failed: {error}</h3><a href='/auth/login'>Try again</a>", status_code=400)

    if not code or not state:
        return HTMLResponse(content="<h3>Invalid OAuth callback parameters</h3><a href='/auth/login'>Try again</a>", status_code=400)

    # Verify state cookie
    state_cookie = request.cookies.get(AUTH_STATE_COOKIE)
    expected_state = _verify_and_unpack(state_cookie) if state_cookie and "." not in state_cookie else None
    # For state check, verify signature
    if not state_cookie or "." not in state_cookie:
        return HTMLResponse(content="<h3>OAuth state session expired.</h3><a href='/auth/login'>Please log in again</a>", status_code=400)
    raw_state, sig = state_cookie.rsplit(".", 1)
    expected_sig = hmac.new(AUTH_SECRET_KEY.encode("utf-8"), raw_state.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig) or raw_state != state:
        return HTMLResponse(content="<h3>Invalid OAuth state parameter.</h3><a href='/auth/login'>Please log in again</a>", status_code=400)

    # Exchange authorization code for tokens
    token_url = "https://oauth2.googleapis.com/token"
    token_data = urllib.parse.urlencode({
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": _get_redirect_uri(request),
        "grant_type": "authorization_code",
    }).encode("utf-8")

    try:
        req = urllib.request.Request(token_url, data=token_data, method="POST", headers={
            "Content-Type": "application/x-www-form-urlencoded"
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            token_resp = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.error("OAuth token exchange failed: %s", e)
        return HTMLResponse(content="<h3>Failed to exchange authorization code with Google.</h3><a href='/auth/login'>Try again</a>", status_code=502)

    id_token_jwt = token_resp.get("id_token")
    if not id_token_jwt:
        return HTMLResponse(content="<h3>No ID token returned by Google.</h3><a href='/auth/login'>Try again</a>", status_code=502)

    user_info = verify_google_id_token(id_token_jwt)
    if not user_info:
        return HTMLResponse(content="<h3>Invalid ID token signature from Google.</h3><a href='/auth/login'>Try again</a>", status_code=401)

    email = user_info.get("email", "")
    hd = user_info.get("hd", "")

    if not is_user_authorized(email, hd):
        logger.warning("Unauthorized login attempt by %s (hd=%s)", email, hd)
        return HTMLResponse(
            content=f"""
            <div style="font-family: system-ui, sans-serif; max-width: 500px; margin: 80px auto; padding: 30px; border: 1px solid #ff4444; border-radius: 8px; text-align: center; background: #fff5f5;">
                <h2 style="color: #cc0000; margin-top: 0;">Access Denied</h2>
                <p>Your Google account <strong>{email}</strong> is not authorized to access this FinOps application.</p>
                <p style="font-size: 13px; color: #666;">Please sign in with an authorized organizational account.</p>
                <div style="margin-top: 20px;">
                    <a href="/auth/login" style="background: #1a73e8; color: #fff; padding: 10px 20px; border-radius: 4px; text-decoration: none; font-weight: 500;">Sign in with different account</a>
                </div>
            </div>
            """,
            status_code=403
        )

    # Authorized: set session cookie and redirect to app home
    cookie_val = create_session_cookie_value(user_info)
    is_secure = request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https"
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        AUTH_SESSION_COOKIE,
        cookie_val,
        max_age=SESSION_MAX_AGE_SECONDS,
        httponly=True,
        samesite="lax",
        secure=is_secure,
    )
    response.delete_cookie(AUTH_STATE_COOKIE)
    return response


@router.get("/logout")
def logout(request: Request):
    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie(AUTH_SESSION_COOKIE)
    return response


@router.get("/me")
def current_user(request: Request):
    user = get_current_user_from_request(request)
    if not user:
        return JSONResponse({"authenticated": False, "auth_enabled": is_auth_enabled()})
    return JSONResponse({
        "authenticated": True,
        "auth_enabled": is_auth_enabled(),
        "email": user.get("email"),
        "name": user.get("name"),
        "picture": user.get("picture"),
    })


# ---------------------------------------------------------------------------
# Authentication Middleware
# ---------------------------------------------------------------------------

class GoogleAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not is_auth_enabled():
            return await call_next(request)

        path = request.url.path

        # Unrestricted paths
        if (
            path.startswith("/auth/")
            or path.startswith("/static/")
            or path in ("/favicon.ico", "/favicon.png", "/docs", "/openapi.json", "/health")
        ):
            return await call_next(request)

        user = get_current_user_from_request(request)
        if user:
            # Store in request state for downstream handlers if needed
            request.state.user = user
            return await call_next(request)

        # Unauthenticated: HTML requests redirect to /auth/login; API requests return 401
        accept = request.headers.get("accept", "")
        if "text/html" in accept or path == "/":
            return RedirectResponse(url="/auth/login", status_code=302)

        return JSONResponse(
            status_code=401,
            content={"detail": "Authentication required. Please log in with Google at /auth/login."}
        )

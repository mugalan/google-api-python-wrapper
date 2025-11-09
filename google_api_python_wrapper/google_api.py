# google_oauth_services.py

import os
import json
import sys
from pathlib import Path
from typing import Optional, Tuple, Any

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from dataclasses import dataclass


# ---- Edit scopes once ----
SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/calendar",
    "https://www.googleapis.com/auth/tasks",
    "https://www.googleapis.com/auth/forms",
]

# Defaults (you can override via function args/env)
DEFAULT_OAUTH_CLIENT_FILE = "oauth-client.json"      # "Desktop app" client JSON
DEFAULT_TOKEN_STEM = "oOne_token"                         # token file name stem
DEFAULT_TOKEN_DIR = os.getenv("GOOGLE_OAUTH_TOKEN_DIR") or "utilities"
ENV_CLIENT_INFO = "GOOGLE_OAUTH_CLIENT_INFO"         # JSON (from GCP OAuth client)

# ----------------- helpers -----------------

def _in_colab() -> bool:
    try:
        import google.colab  # noqa: F401
        return True
    except Exception:
        return False

def _token_dir() -> Path:
    d = Path(DEFAULT_TOKEN_DIR)
    d.mkdir(parents=True, exist_ok=True)
    return d

def _token_path(stem: str) -> Path:
    return _token_dir() / f"{stem}.json"

def _load_token(stem: str) -> Optional[Credentials]:
    p = _token_path(stem)
    if not p.exists():
        return None
    data = json.loads(p.read_text())
    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            _save_token(stem, creds)
        except Exception:
            # Refresh failed (revoked/changed password/etc). Force re-consent.
            return None
    return creds

def _save_token(stem: str, creds: Credentials) -> None:
    payload = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    _token_path(stem).write_text(json.dumps(payload))

def _client_secrets_from_env(env_var: str = ENV_CLIENT_INFO) -> Optional[dict]:
    raw = os.getenv(env_var)
    if not raw:
        return None
    return json.loads(raw)

# def _colab_credentials(scopes: list[str]) -> Credentials:
#     """
#     Use Colab's built-in user auth: no client_secret.json, no token files.
#     """
#     from google.colab import auth as colab_auth  # type: ignore
#     colab_auth.authenticate_user()

#     import google.auth
#     creds, _ = google.auth.default()
#     creds = creds.with_scopes(scopes)
#     creds.refresh(Request())
#     return creds

def _colab_credentials(scopes: list[str]) -> Credentials:
    from google.colab import auth as colab_auth  # type: ignore
    colab_auth.authenticate_user()  # re-run this after changing scopes

    import google.auth
    # IMPORTANT: ask for scopes here, not via with_scopes afterwards
    creds, _ = google.auth.default(scopes=scopes)
    # No manual refresh needed; the returned creds are scoped correctly
    return creds

# ----------------- public API -----------------

def get_oauth_credentials(
    *,
    oauth_client_file: Optional[str] = None,
    oauth_token_stem: str = DEFAULT_TOKEN_STEM,
    interactive: Optional[bool] = None,
) -> Credentials:
    """
    Order:
      1) Saved token (refresh + persist)
      2) In Colab + no client info -> Colab creds
      3) interactive=False -> raise
      4) InstalledAppFlow (local browser)
    """
    required_scopes = set(SCOPES)

    def _scopes_ok(c: Optional[Credentials]) -> bool:
        return bool(c and set(c.scopes or []) >= required_scopes)

    # --- 0) Prefer token on disk ---
    creds = _load_token(oauth_token_stem)
    if creds and _scopes_ok(creds):
        if not creds.valid:
            if creds.refresh_token:
                creds.refresh(Request())
                _save_token(oauth_token_stem, creds)
            else:
                creds = None  # fall through
        if creds:
            return creds

    # --- 1) Colab fallback (only if no client info provided) ---
    no_client_info = _client_secrets_from_env() is None and oauth_client_file is None
    if _in_colab() and no_client_info:
        try:
            colab_creds = _colab_credentials(SCOPES)
            # Optional: sanity check scopes
            if set(colab_creds.scopes or []) >= required_scopes:
                return colab_creds
            # If Colab creds don’t include required scopes, keep falling through
        except Exception:
            pass  # fall through to next path

    # --- 2) If we can't prompt, stop here ---
    if interactive is False:
        raise RuntimeError(
            "No valid OAuth token found and interactive=False. "
            f"Place a token at {_token_path(oauth_token_stem)} or set interactive=True."
        )

    # --- 3) Run Installed App flow (local browser) ---
    client_info = _client_secrets_from_env()
    if client_info:
        flow = InstalledAppFlow.from_client_config(client_info, SCOPES)
    else:
        client_file = oauth_client_file or DEFAULT_OAUTH_CLIENT_FILE
        if not Path(client_file).exists():
            raise FileNotFoundError(
                f"OAuth client secrets not found at {client_file} and {ENV_CLIENT_INFO} not set. "
                "Use a Desktop app client JSON."
            )
        flow = InstalledAppFlow.from_client_secrets_file(client_file, SCOPES)

    creds = flow.run_local_server(port=0, access_type="offline", include_granted_scopes=True)
    _save_token(oauth_token_stem, creds)
    return creds



def get_google_services_oauth(
    *,
    oauth_client_file: Optional[str] = None,
    oauth_token_stem: str = DEFAULT_TOKEN_STEM,
    interactive: Optional[bool] = None,
) -> Tuple:
    """
    Returns Drive, Docs, Sheets, Calendar, Tasks, Forms clients using OAuth.
    Auto-detects Colab and uses Colab auth unless you provide client info.
    """
    creds = get_oauth_credentials(
        oauth_client_file=oauth_client_file,
        oauth_token_stem=oauth_token_stem,
        interactive=interactive,
    )
    drive    = build("drive",    "v3", credentials=creds)
    docs     = build("docs",     "v1", credentials=creds)
    sheets   = build("sheets",   "v4", credentials=creds)
    calendar = build("calendar", "v3", credentials=creds)
    tasks    = build("tasks",    "v1", credentials=creds)
    forms    = build("forms",    "v1", credentials=creds)
    gmail = build("gmail", "v1", credentials=creds)
    return drive, docs, sheets, calendar, tasks, forms, gmail

@dataclass
class GoogleAuthResult:
    """Outcome of attempting to create Google API service clients."""
    services: Optional[Tuple[Any, ...]] = None
    auth_failed: bool = False          # True if auth failed for any reason
    error: Optional[Exception] = None  # Non-auth error that occurred (if any)

    @property
    def ok(self) -> bool:
        return not self.auth_failed and self.services is not None


def try_get_google_services_oauth(
    *,
    oauth_client_file: Optional[str] = None,
    oauth_token_stem: str = DEFAULT_TOKEN_STEM,
    interactive: Optional[bool] = None,
) -> GoogleAuthResult:
    """
    Wraps get_google_services_oauth:
      - Returns GoogleAuthResult with auth_failed=True if authentication failed
        (i.e., underlying helper returned None).
      - Captures any non-auth exceptions in .error and also marks auth_failed=True.
    """
    try:
        services = get_google_services_oauth(
            oauth_client_file=oauth_client_file,
            oauth_token_stem=oauth_token_stem,
            interactive=interactive,
        )
        return GoogleAuthResult(services=services, auth_failed=False, error=None)
    except Exception as e:
        # Non-auth failure (e.g., network, misconfig). Flag and attach error.
        return GoogleAuthResult(services=None, auth_failed=True, error=e)
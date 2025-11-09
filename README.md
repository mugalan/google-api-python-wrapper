# google-api-python-wrapper

A lightweight convenience wrapper around Google APIs (Drive, Docs, Sheets, Calendar, Tasks, Forms, Gmail). It centralizes OAuth and exposes a single class with handy, task-focused methods so you can script common workflows in a few lines.

---

## ✨ Features

* **Unified auth**: one place to sign in and build all service clients
* **Drive**: explore folders, create folders, upload/download/copy/move files, recursive folder copy
* **Docs**: create a Doc, write Markdown-ish content, extract Markdown
* **Sheets**: read every sheet as records, append a DataFrame as a new tab
* **Calendar**: create events (incl. Google Meet), list between dates, delete by IDs
* **Tasks**: create a task with optional due time
* **Forms**: create a form, set email collection, add questions, fetch responses
* **Gmail**: send a plain‑text email via API

> After initialization, check `obj.google_auth` and `obj.error`. Available services: `drive_service, docs_service, sheets_service, calendar_service, tasks_service, forms_service, gmail_service`.

---

## 📦 Installation

### Option A — Install directly from GitHub (recommended)

```bash
pip install "git+https://github.com/mugalan/google-api-python-wrapper.git"
```

### Option B — From a local clone

```bash
git clone https://github.com/mugalan/google-api-python-wrapper.git
cd google-api-python-wrapper
pip install -e .
```

> Python 3.10+ is recommended. Ensure you have a Google Cloud project and **OAuth client credentials** JSON (Desktop App) downloaded.

---

## 🔑 Authentication

The helper supports **three** auth paths. You do **not** need to pass `oauth_client_file` if a valid token exists or if you’re in Colab and want to use Colab’s built‑in auth.

### 1) Silent token reuse (no secrets)

If a token exists at `utilities/<TOKEN_STEM>.json` (or in a custom dir), it will be loaded and refreshed **silently**.

```python
from google_api_python_wrapper import GoogleApi

api = GoogleApi(
    oauth_token_stem="oOne_token",   # or your chosen stem
    interactive=False,                # optional; silent auth using token
)
assert api.google_auth, api.error
```

### 2) Colab built-in auth (no secrets, no token)

If you are in **Colab** and **no client info** is provided (no env var and no file), the helper falls back to **Colab user auth** and requests all required scopes. Note that `calendar`, `forms` and `task` are not included in this authorization flow.

```python
# In Colab:
from google_api_python_wrapper import GoogleApi
api = GoogleApi()   # no args needed in Colab for built-in auth
assert api.google_auth, api.error
```

> This uses your signed‑in Colab Google account. Rerun auth if you change scopes.

### 3) Installed-app flow (creates a reusable token)

Use your OAuth Client (Desktop) JSON or set `GOOGLE_OAUTH_CLIENT_INFO` with the JSON. This path creates `utilities/<TOKEN_STEM>.json` for future silent auth.

```python
import os, json
from google_api_python_wrapper import GoogleApi

# Option A: secrets via file (defaults to 'oauth-client.json' if not provided)
api = GoogleApi(oauth_client_file="oauth-client.json", oauth_token_stem="oOne_token", interactive=True)

# Option B: secrets via env var (no file on disk)
os.environ["GOOGLE_OAUTH_CLIENT_INFO"] = json.dumps({
  "installed": {"client_id": "...","project_id": "...","auth_uri": "...",
                 "token_uri": "...","client_secret": "...","redirect_uris": ["http://localhost"]}
}})
api = GoogleApi(oauth_token_stem="oOne_token", interactive=True)
```

---

## 🧰 Quick start

```python
from google_api_python_wrapper import GoogleApi
api = GoogleApi()  # Colab: uses built-in auth; Local: try token then fallback if configured
assert api.google_auth, api.error
```

---

## 📁 Google Drive — examples

### 1) Explore a folder (non‑recursive)

```python
resp = api.get_gdrive_folder_explorer(
    folder_id="root",           # or a specific folder ID
    query="report",             # optional substring match on name
    mime_types=["application/pdf", "application/vnd.google-apps.folder"],
    only_folders=False,
    page_size=10,
)
print(resp["message"])                    # human‑readable summary
df = pd.DataFrame(json.loads(resp["response"]["data"]).get('records',[]))     # A pandas DataFrame of {"records": [...]}
```

### 2) Create a folder

```python
resp = api.create_gdrive_folder(name="ProjectDocs")
print(resp["message"])  # Folder "ProjectDocs" created with ID: ...
new_folder_id = resp["response"]["meta_data"]["id"]
```

### 3) Upload a file

```python
resp = api.upload_file_to_drive(
    file_path="/path/to/File.pdf",
    parent_folder_id=new_folder_id
)
print(resp["message"])  # File "File.pdf" uploaded with ID: ...
```

### 4) Move a file to a folder

```python
api.move_gdrive_file_to_folder(file_id="<FILE_ID>", folder_id=new_folder_id)
```

### 5) Copy a folder recursively (skip newer destination files)

```python
resp = api.copy_gdrive_folder_recursive(
    source_folder_id="<SRC_FOLDER_ID>",
    destination_parent_folder_id="<DEST_PARENT_ID>",
    new_folder_name="Backup_2025"
)
print(resp["response"]["message"])  # multi‑line log
```

---

## 📝 Google Docs — examples

### 1) Create a Google Doc

```python
resp = api.create_gdoc(title="Meeting Notes", parent_folder_id=new_folder_id)
print(resp["response"]["message"])  # Document "Meeting Notes" created with ID: ...
doc_id = resp["response"]["meta_data"]["doc_id"]
```

### 2) Write Markdown-ish content to the Doc

```python
markdown = """
# Project Apollo

## Status
**On track** for _Phase 2_. See [spec](https://example.com/spec).

- Milestone A
- Milestone B
"""

api.write_markdown_content(doc_id=doc_id, markdown_text=markdown)
```

### 3) Extract Markdown from a Doc

```python
out = api.extract_markdown_from_doc(doc_id=doc_id)
print(out["response"]["data"])  # markdown string
```

---

## 📊 Google Sheets — examples

### 1) Read every sheet in a spreadsheet

```python
s = api.get_all_sheets_in_a_google_sheet(spreadsheet_id="<SHEET_ID>")
print(s["message"])      # log per sheet
df = pd.DataFrame(json.loads(resp["response"]["data"]).get('records',[]))     # A pandas DataFrame of {"records": [...]}
```

### 2) Add a DataFrame as a new tab

```python
payload = [
    {"name": "Alice", "score": 95},
    {"name": "Bob",   "score": 88},
]
api.add_dataframe_as_new_sheet(
    spreadsheet_id="<SHEET_ID>",
    data=payload,
    new_sheet_name="Results_2025_11_09",
)
```

---

## 📆 Google Calendar — examples

### 1) Create an event (with optional Google Meet)

```python
from datetime import datetime, timedelta, timezone
start = datetime.now(timezone.utc).replace(microsecond=0)
end = start + timedelta(hours=1)

resp = api.create_google_calendar_event(
    summary="Team Sync",
    start_time=start.isoformat(),
    end_time=end.isoformat(),
    description="Weekly status sync",
    timezone="UTC",
    attendees_emails=["alice@example.com"],
    use_google_meet=True,
)
print(resp["response"]["message"])  # links
```

### 2) List events between two dates

```python
resp = api.get_google_calendar_events_between(
    start_time="2025-11-01T00:00:00Z",
    end_time="2025-11-30T23:59:59Z",
    calendar_id="primary",
)
print(resp["response"]["message"])           # e.g., "Retrieved 5 event(s)."
records = resp["response"]["data"]           # JSON string of events
```

### 3) Delete by event IDs

```python
api.delete_google_calendar_events_by_ids([
    "<EVENT_ID_1>",
    "<EVENT_ID_2>",
])
```

---

## ✅ Google Tasks — example

```python
api.create_google_task(
    title="Prepare Q4 report",
    notes="Gather metrics and draft executive summary.",
    due="2025-12-01T17:00:00Z",
)
```

---

## 📮 Gmail — example (send email)

```python
api.send_email(
    sender="me@gmail.com",
    to=["you@example.com", "them@example.com"],
    subject="Test",
    body="Hello from the API wrapper",
)
```

> Requires Gmail scope: `https://www.googleapis.com/auth/gmail.send`.

---

## 📋 Google Forms — examples

### 1) Create a form and set email collection

```python
created = api.create_google_form(
    title="Event Feedback",
    email_collection_mode="VERIFIED",  # or RESPONDER_INPUT / DO_NOT_COLLECT
)
form_id = created.get("meta_data", {}).get("formId") or \
          created.get("response", {}).get("data")
print(created["message"])  # includes responder URL
```

### 2) Add a multiple‑choice question

```python
requests = [
    {
        "createItem": {
            "item": {
                "title": "Your favorite color?",
                "questionItem": {
                    "question": {
                        "required": True,
                        "choiceQuestion": {
                            "type": "RADIO",
                            "options": [{"value": "Red"}, {"value": "Blue"}, {"value": "Green"}],
                            "shuffle": False
                        }
                    }
                }
            },
            "location": {"index": 0}
        }
    }
]
api.add_questions_to_google_form(form_id="<FORM_ID>", questions=requests)
```

### 3) Fetch responses

```python
resp = api.get_google_form_responses(form_id="<FORM_ID>")
print(resp["message"])                 # e.g., "Retrieved N response(s)."
records_json = resp["response"]["data"]
```

---

## 💡 Tips & gotchas

* **Token location**: tokens are saved to `utilities/<TOKEN_STEM>.json` by default. Override the directory with env var `GOOGLE_OAUTH_TOKEN_DIR`.
* **Colab persistence**: set `GOOGLE_OAUTH_TOKEN_DIR` to a path in Drive to persist across sessions (see cheat sheet below).
* **Scopes**: if you call a method that needs a scope your token lacks, delete the token JSON and re‑auth with broader scopes.
* Call `api.ensure_auth()` if token files appear after construction.
* Drive copy/move needs sufficient permissions for source + destination.
* Large Drive trees may hit rate limits—batch and backoff help.

---

## 🧪 Minimal sanity check

```python
api = GoogleApi()
assert api.google_auth, api.error
print("Drive ready?", bool(api.drive_service))
print("Docs ready?", bool(api.docs_service))
print("Sheets ready?", bool(api.sheets_service))
print("Calendar ready?", bool(api.calendar_service))
```

---

## 🛠 Troubleshooting

* **Use Colab without secrets**: simply `api = GoogleApi()`; it will prompt using Colab’s built-in auth if needed.
* **Silent reuse not happening**: ensure the token exists at `utilities/<TOKEN_STEM>.json` (or your custom dir) and you passed the same `oauth_token_stem`.
* **Persist tokens in Colab**: mount Drive and set `GOOGLE_OAUTH_TOKEN_DIR` *before* constructing the API.

### Colab auth cheat sheet

```python
# 1) Persist tokens in Drive
from google.colab import drive
drive.mount('/content/drive')

import os
os.environ['GOOGLE_OAUTH_TOKEN_DIR'] = '/content/drive/MyDrive/colab_tokens'  # directory, not a file

# 2) First run (Colab built-in; no secrets):
from google_api_python_wrapper import googleApiMethods as GoogleApi
api = GoogleApi(oauth_token_stem='oOne_token')
assert api.google_auth, api.error

# 3) Subsequent runs: silent reuse
api = GoogleApi(oauth_token_stem='oOne_token', interactive=False)
assert api.google_auth, api.error
```

* **Missing/insufficient scopes**: re-auth in Colab (re-run, or delete token JSON if you’re using token auth) and ensure the scope list includes what you need.
* **Switch account**: delete the token JSON in your token directory and re-auth.
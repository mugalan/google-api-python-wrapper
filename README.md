# google-api-python-wrapper

A lightweight convenience wrapper around Google APIs (Drive, Docs, Sheets, Calendar, Tasks, Forms, Gmail). It centralizes OAuth and exposes a single class with handy, task‑focused methods so you can script common workflows in a few lines.

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

The library supports interactive OAuth (ideal for local/Colab) and token‑file reuse.

### Files you need

* **OAuth client file**: e.g. `client_secret_XXXXX.apps.googleusercontent.com.json`
* **Token file(s)**: created on first successful sign‑in; stem defaults to `token` (e.g., `token.json`). You can change the stem via `oauth_token_stem`.

### Colab / Local (interactive flow)

In Google Colab, upload your OAuth client JSON to the runtime first.

```python
from google_api_python_wrapper import googleApiMethods as GoogleApi

api = GoogleApi(
    oauth_client_file="client_secret.json",   # uploaded/available path
    oauth_token_stem="token",                # will create token.json on first auth
    interactive=True,                         # show browser consent flow
)

if not api.google_auth:
    raise RuntimeError(api.error or "OAuth failed")
```

### Headless / Reusing an existing token

If a valid token file (e.g., `token.json`) already exists on disk, you can authenticate without interactivity:

```python
api = GoogleApi(
    oauth_client_file="client_secret.json",
    oauth_token_stem="token",
    interactive=False,   # try silent auth using existing token
)

# Later, if a token appears after construction:
api.ensure_auth()
```

> **Scopes** are requested according to the APIs you use (Drive/Docs/Sheets/Calendar/Tasks/Forms/Gmail). For Gmail send, ensure the project allows the scope `https://www.googleapis.com/auth/gmail.send`.

---

## 🧰 Quick start

```python
from google_api_python_wrapper import googleApiMethods as GoogleApi

api = GoogleApi(oauth_client_file="client_secret.json", interactive=True)
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
records = (resp["response"]["data"])     # JSON string of {"records": [...]}
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

### 2) Write Markdown‑ish content to the Doc

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
data_json = s["response"]["data"]
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

* Call `api.ensure_auth()` if token files are created after constructing the object.
* Many methods return a structure with keys like `status`, `response.meta_data`, `response.data` (often a JSON string), and `message`.
* Drive copy/move methods assume you have sufficient permissions on both source and destination.
* For large Drive trees, you may hit rate limits—consider batching and backoff.

---

## 🧪 Minimal sanity check

```python
api = GoogleApi(oauth_client_file="client_secret.json", interactive=True)
assert api.google_auth, api.error
print("Drive ready?", bool(api.drive_service))
print("Docs ready?", bool(api.docs_service))
print("Sheets ready?", bool(api.sheets_service))
print("Calendar ready?", bool(api.calendar_service))
```

---

## 🛠 Troubleshooting

* **`invalid_grant` / consent errors**: Ensure you’re using a Desktop OAuth client; add your test user under OAuth consent screen if the app is in testing mode.
* **`403 insufficient permissions`**: Missing scopes; delete token file and re‑authenticate after enabling needed APIs/scopes in the Cloud Console.
* **Colab cannot find `client_secret.json`**: Upload it to the runtime and pass the correct path.

---

## 📄 License

MIT (see repository).

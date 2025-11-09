from .google_api import try_get_google_services_oauth, DEFAULT_TOKEN_STEM
import pandas as pd
import os
import json
import csv
import uuid
from pathlib import Path
import inspect
import re
import io
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from dateutil.tz import UTC
import mimetypes
from bs4 import BeautifulSoup

from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload

from googleapiclient.discovery import build
import base64
from email.mime.text import MIMEText
from typing import Union
from googleapiclient.discovery import Resource


from typing import Optional, Any, Tuple


class GoogleApi:
    """
    Encapsulates Google OAuth + service clients.
    After init, check `self.google_auth` and `self.error`.
    Services (when available): self.drive_service, self.docs_service, self.sheets_service,
    self.calendar_service, self.tasks_service, self.forms_service, self.gmail_service
    """

    def __init__(
        self,
        *,
        oauth_client_file: Optional[str] = None,
        oauth_token_stem: str = DEFAULT_TOKEN_STEM,
        interactive: Optional[bool] = None,
        auto_init: bool = True,
    ) -> None:
        # config
        self._oauth_client_file = oauth_client_file
        self._oauth_token_stem = oauth_token_stem
        self._interactive = interactive

        # state
        self.google_auth: bool = False
        self.error: Optional[Exception] = None

        # services
        self.drive_service: Any = None
        self.docs_service: Any = None
        self.sheets_service: Any = None
        self.calendar_service: Any = None
        self.tasks_service: Any = None
        self.forms_service: Any = None
        self.gmail_service: Any = None

        if auto_init:
            self.init_auth()

    def init_auth(self) -> bool:
        """
        Attempts to obtain OAuth creds + build services.
        Sets self.google_auth and self.error.
        Returns True on success, False otherwise.
        """
        res = try_get_google_services_oauth(
            oauth_client_file=self._oauth_client_file,
            oauth_token_stem=self._oauth_token_stem,
            interactive=self._interactive,
        )
        self.google_auth = bool(res and res.ok)
        self.error = getattr(res, "error", None)

        if self.google_auth:
            (self.drive_service, self.docs_service, self.sheets_service,
             self.calendar_service, self.tasks_service, self.forms_service, self.gmail_service) = res.services
        else:
            # ensure all are None on failure
            self.drive_service = self.docs_service = self.sheets_service = None
            self.calendar_service = self.tasks_service = self.forms_service = None
            self.gmail_service = None
        return self.google_auth


    def ensure_auth(self) -> bool:
        """
        Lazy ensure: if not authenticated, try again once.
        Useful if token file appeared after construction.
        """
        return self.google_auth or self.init_auth()

    def services_tuple(self) -> Optional[Tuple[Any, ...]]:
        """Return services as a tuple (matching your original order) or None."""
        if not self.google_auth:
            return None
        return (self.drive_service, self.docs_service, self.sheets_service,
                self.calendar_service, self.tasks_service, self.forms_service, self.gmail_service)

    

    def send_email(self, sender: str, to: Union[str, list[str]], subject: str, body: str):
        """
        Sends a plain text email using the Gmail API.

        This method constructs a MIME message, encodes it in base64, and sends it using the Gmail API.
        The `gmail_service` must be an authenticated Gmail API client.

        Parameters:
            sender (str): The sender's email address.
            to (str | list[str]): One or more recipient email addresses.
            subject (str): The subject of the email.
            body (str): The plain text body of the email.

        Returns:
            dict: A dictionary containing:
                - 'status' (str): 'success' if the email was sent, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes the recipient(s) and message ID.
                    - 'data' (str): JSON-encoded string containing the message metadata.
                    - 'message' (str): A human-readable message about the result.

        Example:
            >>> send_email(gmail_service, 'me@gmail.com', ['you@example.com', 'them@example.com'], 'Test', 'Hello')
            {
                'status': 'success',
                'response': {
                    'meta_data': {'to': ['you@example.com', 'them@example.com'], 'id': '17abcd123xyz'},
                    'data': '{"records": [{"to": ["you@example.com", "them@example.com"], "id": "17abcd123xyz"}]}',
                    'message': 'Email sent to 2 recipient(s) with ID: 17abcd123xyz'
                }
            }

        Notes:
            - The `to` field can be a single string or a list of email addresses.
            - The email body is plain text. Use MIME multipart for HTML or attachments if needed.
            - Gmail API requires 'https://www.googleapis.com/auth/gmail.send' scope.
        """
        status = ''
        message = ''
        meta_data = {}

        try:
            if isinstance(to, list):
                to_str = ', '.join(to)
                recipients = to
            else:
                to_str = to
                recipients = [to]

            mime_msg = MIMEText(body)
            mime_msg['to'] = to_str
            mime_msg['from'] = sender
            mime_msg['subject'] = subject

            raw_msg = base64.urlsafe_b64encode(mime_msg.as_bytes()).decode()
            send_result = self.gmail_service.users().messages().send(userId='me', body={'raw': raw_msg}).execute()

            msg_id = send_result.get('id')
            status = 'success'
            message = f'Email sent to {len(recipients)} recipient(s) with ID: {msg_id}'
            meta_data = {'to': recipients, 'id': msg_id}

        except Exception as e:
            status = 'error'
            message = f'Error: {str(e)}'
            meta_data = {'to': to, 'id': None}

        response = {
            'meta_data': meta_data,
            'data': json.dumps({"records": [meta_data]}),
            'message': message
        }

        return {
            'status': status,
            'response': response,
            'message': message
        }

    def get_gdrive_folder_explorer(
        self,
        folder_id: str = "root",
        query: str | None = None,
        user_id: str | None = None,               # kept for your signature
        *,
        mime_types: str | list[str] | tuple[str, ...] | None = None,
        only_folders: bool = False,
        shared_drive_id: str | None = None,       # pass a drive ID to target a specific Shared Drive
        page_size: int =10
    ):
        """
        Fetch contents of a Google Drive folder with optional name and MIME-type filtering using the Google Drive API (v3) .
        (Non-recursive.)

        Args:
            folder_id: Drive folder ID to explore; 'root' for My Drive root.
            query:     Plain search term for name filtering (uses "name contains '<term>'").
            mime_types: One MIME type (str) or a list/tuple of MIME types to include.
            only_folders: If True, include only folders.
            shared_drive_id: If set, search that Shared Drive (Team Drive). When provided,
                            the call sets corpora="drive" and driveId=<shared_drive_id>.
            page_size: Number of records to return. Default is 10.

        Returns:
            dict with 'status', 'response' (meta_data, data={'records':[...]}, message), and 'message'.
        """
        status = "error"
        meta_data = {}
        records: list[dict] = []
        message = ""

        # ---- helpers -------------------------------------------------------------
        def _escape_term(s: str) -> str:
            # Drive 'q' strings are single-quoted with backslash escapes for quotes/backslashes.
            return s.replace("\\", "\\\\").replace("'", "\\'")

        def _normalize_mimes(m) -> list[str]:
            if m is None:
                return []
            if isinstance(m, (list, tuple)):
                return list(m)
            if isinstance(m, str):
                return [m]
            raise TypeError("mime_types must be None, str, list[str], or tuple[str,...]")


        try:
            parts: list[str] = []

            # Scope by parent (unless you intentionally want to search all of My Drive)
            if folder_id and folder_id != "root":
                parts.append(f"'{folder_id}' in parents")

            # Name filter
            if query:
                parts.append(f"name contains '{_escape_term(query)}'")

            # Trashed
            parts.append("trashed = false")

            # MIME type filters
            if only_folders:
                parts.append("mimeType = 'application/vnd.google-apps.folder'")
            elif mime_types:
                mts = _normalize_mimes(mime_types)
                if mts:
                    or_block = " or ".join([f"mimeType = '{_escape_term(mt)}'" for mt in mts])
                    parts.append(f"({or_block})")
            # else: include both files and folders (no filter)


            q = " and ".join(parts)

            # ---- list() params (Shared Drives friendly defaults) -----------------
            list_kwargs = dict(
                q=q,
                pageSize=page_size,
                fields="nextPageToken, files(id,name,mimeType,parents,modifiedTime,webViewLink,iconLink)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
                orderBy="recency desc",
            )
            if shared_drive_id:
                # Search a specific shared drive rather than 'user' (My Drive).
                list_kwargs.update(corpora="drive", driveId=shared_drive_id)

            # ---- pagination ------------------------------------------------------
            page_token = None
            while True:
                if page_token:
                    list_kwargs["pageToken"] = page_token
                resp = self.drive_service.files().list(**list_kwargs).execute()
                for f in resp.get("files", []):
                    records.append(
                        {
                            "id": f.get("id"),
                            "name": f.get("name"),
                            "mimeType": f.get("mimeType"),
                            "modifiedTime": f.get("modifiedTime"),
                            "webViewLink": f.get("webViewLink"),
                            "iconLink": f.get("iconLink"),
                        }
                    )
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            # ---- response assembly ----------------------------------------------
            meta_data = {
                "folder_id": folder_id,
                "search": query,
                "mime_types": _normalize_mimes(mime_types) if not (only_folders) else None,
                "only_folders": only_folders,
                "shared_drive_id": shared_drive_id,
                "q": q,  # handy for debugging
            }

            message = f"Found {len(records)} item(s)"
            if query:
                message += f" matching \"{query}\""
            if only_folders:
                message += " (folders only)"
            elif mime_types:
                message += f" (mime_types filter applied)"

            # Pretty list
            if records:
                message += ":\n" + "\n".join(
                    f"- {r['name']} {'📁' if r['mimeType']=='application/vnd.google-apps.folder' else '📄'} (id: {r['id']}, mime_type: {r['mimeType']})"
                    for r in records
                )

            status = "success"

        except Exception as e:
            message = f"Error: {e!s}"
            status = "error"

        return {
            "status": status,
            "response": {
                "meta_data": meta_data,
                "data": json.dumps({"records": records}),   # structured dict (not JSON string)
                "message": message,
            },
            "message": message,
    }

    def create_gdrive_folder(self, name=None, parent_folder_id=None, user_id=None):
        """
        Creates a new folder in Google Drive, optionally inside a specified parent folder.

        This method uses the Google Drive API to create a new folder with the specified `name`. 
        If a `parent_folder_id` is provided, the new folder will be nested inside it.

        Parameters:
            name (str): The name of the folder to be created.
            parent_folder_id (str, optional): The ID of the parent folder where the new folder will be created. 
                                            If not provided, the folder will be created at the root level.

        Returns:
            dict: A dictionary containing:
                - 'status' (str): 'success' if the folder was created, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes the name and ID of the created folder (if successful).
                    - 'data' (str): JSON-encoded string containing the folder metadata.
                    - 'message' (str): A human-readable message about the result.

        Example:
            >>> create_gdrive_folder(name='ProjectDocs', parent_folder_id='1a2b3c4d')
            {
                'status': 'success',
                'response': {
                    'meta_data': {'folder': 'ProjectDocs', 'id': 'abc123xyz'},
                    'data': '{"records": {"folder": "ProjectDocs", "id": "abc123xyz"}}',
                    'message': 'Folder "ProjectDocs" created with ID: abc123xyz'
                }
            }

        Notes:
            - The `mimeType` used is `'application/vnd.google-apps.folder'` which is required for Drive folders.
            - The Google Drive API service (`drive_service`) must be authenticated and accessible.
            - Any exceptions during folder creation are caught and returned in the response message.

        Raises:
            Exception: Any errors from the Drive API are caught and returned as error messages in the response.
        """
        status=''
        message=''
        meta_data = {}
        try:
            folder_metadata = {
                'name': name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]

            folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            message = f'Folder "{name}" created with ID: {folder_id}'
            status = 'success'
        except Exception as e:
            message = f'Error: {str(e)}'
            

        meta_data = {'folder':name, 'id':folder_id}
        response = {"meta_data": meta_data, "data":json.dumps({"records":[meta_data]}), "message":message}
        response = {
            'status': status,
            'response':response,
            "message":message
        } 

        return response        

    def upload_file_to_drive(self, file_path, file_name=None, parent_folder_id=None, user_id=None):
        """
        Uploads a file to Google Drive, optionally inside a specified parent folder.

        This method uses the Google Drive API to upload a file. If a `parent_folder_id` is provided,
        the file will be uploaded into that folder. If `file_name` is not specified, the original
        filename from `file_path` will be used.

        Parameters:
            file_path (str): Path to the local file to be uploaded.
            file_name (str, optional): Desired name of the file on Google Drive.
            parent_folder_id (str, optional): ID of the folder to upload the file into.
            user_id (str, optional): Optional user identifier for tracking/logging.

        Returns:
            dict: A dictionary with:
                - 'status' (str): 'success' if the upload was successful, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes `parent_folder_id`, `file_id`, and `file_name`.
                    - 'data' (str): JSON-encoded version of the metadata under the `records` key.
                    - 'message' (str): Human-readable message.

        Example:
            >>> upload_file_to_drive('/path/to/file.pdf', parent_folder_id='1AbCdEfGhIj')
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'parent_folder_id': '1AbCdEfGhIj',
                        'file_id': '1XyZ9aBcDeFgHiJkLmNo',
                        'file_name': 'file.pdf'
                    },
                    'data': '{"records":[{"parent_folder_id": "1AbCdEfGhIj", "file_id": "1XyZ9aBcDeFgHiJkLmNo", "file_name": "file.pdf"}]}',
                    'message': 'File "file.pdf" uploaded with ID: 1XyZ9aBcDeFgHiJkLmNo'
                }
            }

        Notes:
            - The Drive API service (`drive_service`) must be authenticated.
            - Automatically detects MIME type using `mimetypes` module.
            - Supports common file types (PDF, image, text, etc.).

        Raises:
            Exception: Any Drive API errors are caught and returned in the message.
        """


        status = 'success'
        message = ''
        file_id = ''
        meta_data = {}

        try:
            if not file_name:
                file_name = os.path.basename(file_path)

            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                mime_type = 'application/octet-stream'

            file_metadata = {
                'name': file_name
            }
            if parent_folder_id:
                file_metadata['parents'] = [parent_folder_id]

            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            uploaded_file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            file_id = uploaded_file.get('id')
            message = f'File "{file_name}" uploaded with ID: {file_id}'

        except Exception as e:
            status = 'error'
            message = f'Error: {str(e)}'

        meta_data = {
            'parent_folder_id': parent_folder_id,
            'file_id': file_id,
            'file_name': file_name
        }

        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records": [meta_data]}),
                'message': message
            },
            'message': message
        }

    def create_gdoc(self, title=None, parent_folder_id=None, user_id=None):
        """
        Creates a new Google Docs document in Google Drive, optionally inside a specified parent folder.

        This method uses the Google Drive API to create a blank Google Docs document with a specified title.
        If a `parent_folder_id` is provided, the document will be created inside that folder.

        Parameters:
            title (str): The title of the Google Docs document to be created.
            parent_folder_id (str, optional): The ID of the parent folder where the document should be created.
                                            If not provided, the document will be created at the root level.

        Returns:
            dict: A dictionary with:
                - 'status' (str): 'success' if the document was created, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes `parent_folder_id`, `doc_id`, and `title` of the created document.
                    - 'data' (str): JSON-encoded version of the document metadata under the `records` key.
                    - 'message' (str): Human-readable message about the result.

        Example:
            >>> create_gdoc(title='Meeting Notes', parent_folder_id='1AbCdEfGhIj')
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'parent_folder_id': '1AbCdEfGhIj',
                        'doc_id': '1XyZ9aBcDeFgHiJkLmNo',
                        'title': 'Meeting Notes'
                    },
                    'data': '{"records":[{"parent_folder_id": "1AbCdEfGhIj", "doc_id": "1XyZ9aBcDeFgHiJkLmNo", "title": "Meeting Notes"}]}',
                    'message': 'Document "Meeting Notes" created with ID: 1XyZ9aBcDeFgHiJkLmNo'
                }
            }

        Notes:
            - Uses MIME type `'application/vnd.google-apps.document'` to specify a Google Docs file.
            - The Drive API service (`drive_service`) must be authenticated.
            - If an error occurs, the `doc_id` will remain empty and the error will be reflected in the message.

        Raises:
            Exception: Any Drive API errors are caught and returned in the message.
        """
        status = 'success'
        message = ''
        meta_data = {} 
        doc_id=''
        try:
            doc_metadata = {
                'name': title,
                'mimeType': 'application/vnd.google-apps.document',
                'parents': [parent_folder_id]
            }
            doc = self.drive_service.files().create(body=doc_metadata, fields='id').execute()
            message=f'Document "{title}" created with ID: {doc.get("id")}'
            doc_id = doc.get('id')
        except Exception as e:
            message = f'Error: {str(e)}'

        meta_data = {
            'parent_folder_id': parent_folder_id,
            'doc_id': doc_id,
            'title': title
        }

        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records":[meta_data]}),
                'message': message
            }
        } 

    def move_gdrive_file_to_folder(self, file_id, folder_id, user_id=None):
        """
        Moves a file in Google Drive to a specified folder.

        This method uses the Google Drive API to move a file (`file_id`) from its current parent folder(s)
        to a new destination folder (`folder_id`). It does so by:
        - Retrieving the file's current parent folders.
        - Adding the new parent.
        - Removing the previous parents.

        Parameters:
            file_id (str): The ID of the file to move.
            folder_id (str): The ID of the destination folder.

        Returns:
            dict: A dictionary with:
                - 'status' (str): 'success' if the operation was successful, 'error' otherwise.
                - 'response' (dict):
                    - 'meta_data' (dict): Contains `file_id`, list of new parents, and previous parents.
                    - 'data' (str): JSON-encoded string containing the metadata.
                    - 'message' (str): Human-readable description of the operation outcome.

        Example:
            >>> move_gdrive_file_to_folder('1AbCdEfG123', '9XyZtUvW456')
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'file_id': '1AbCdEfG123',
                        'new_parents': ['9XyZtUvW456'],
                        'previous_parents': ['4PrEvIoUs456']
                    },
                    'data': '{"records": [{"file_id": "1AbCdEfG123", ...}]}',
                    'message': 'File 1AbCdEfG123 successfully moved to folder 9XyZtUvW456.'
                }
            }

        Notes:
            - This method replaces all previous parent folders with the new one. 
            - The file will no longer be visible in any of its former folders.
            - Requires that the authenticated Drive API user has `writer` or higher permissions on both the file and the destination folder.

        Raises:
            Exception: Any Drive API errors encountered during retrieval or update are caught and returned in the error response.
        """
        try:
            # First get the existing parents
            file = self.drive_service.files().get(fileId=file_id, fields='parents').execute()
            previous_parents = ",".join(file.get('parents', []))

            # Move the file to the new folder
            updated_file = self.drive_service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()

            # Construct metadata and message
            meta_data = {
                "file_id": file_id,
                "new_parents": updated_file.get('parents', []),
                "previous_parents": previous_parents.split(",") if previous_parents else []
            }
            message = f"File {file_id} successfully moved to folder {folder_id}."

            data_json = json.dumps({"records": [meta_data]})

            return {
                "status": "success",
                "response": {
                    "meta_data": meta_data,
                    "data": data_json,
                    "message": message
                }
            }

        except Exception as e:
            return {
                "status": "error",
                "response": {
                    "message": str(e)
                }
            }

    def copy_file_to_gdrive_folder(self, file_id=None, new_folder_id=None, new_name=None, batch=None, user_id=None):
        """
        Copies a file to a specified folder in Google Drive, with overwrite protection and optional batching.

        This method performs a smart file copy to a target folder in Drive. It:
        - Checks if a file with the same name already exists in the destination folder.
        - Compares modification timestamps.
        - Overwrites older versions if needed.
        - Optionally performs the copy as part of a batch request.

        Parameters:
            file_id (str): The ID of the source file to copy.
            new_folder_id (str): The ID of the destination folder.
            new_name (str, optional): The name of the new file. If not provided, the source file’s name is reused.
            batch (googleapiclient.http.BatchHttpRequest, optional): A batch object to which the copy request is added for batched execution.

        Returns:
            dict: A dictionary containing:
                - 'status' (str): 
                    - 'success' if the file was copied,
                    - 'skipped' if the destination file is newer or the same,
                    - 'error' in case of failure.
                - 'response' (dict):
                    - 'meta_data' (dict): Metadata about the operation including `folder_id`, `file_id`, and `new_name`.
                    - 'data' (str): JSON-encoded metadata in the format `{"records": [...]}`.
                    - 'message' (str): A human-readable description of what occurred (e.g., skipped, copied, added to batch).

        Example:
            >>> copy_file_to_gdrive_folder(
                    file_id='1AbCdEfGh',
                    new_folder_id='9XyZtUvW',
                    new_name='Report_Copy.docx'
                )
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'folder_id': '9XyZtUvW',
                        'file_id': '7LmNoPqRs',
                        'new_name': 'Report_Copy.docx'
                    },
                    'data': '{"records": [{"folder_id": "9XyZtUvW", ...}]}',
                    'message': 'Copied file "Report_Copy.docx" (ID: 7LmNoPqRs)'
                }
            }

        Logic Summary:
            1. Retrieves the file name if not provided.
            2. Checks for existing files in the destination folder with the same name.
            3. Compares modification timestamps:
                - If destination is newer or same, skip copy.
                - If destination is older, delete it and proceed to copy.
            4. Copies the file using the Google Drive API.
            5. Adds the request to a batch if `batch` is provided; otherwise executes immediately.

        Notes:
            - The MIME type is inferred from the original file and retained.
            - File overwrite is handled safely based on timestamp comparison (`modifiedTime`).
            - Batch copying enables faster execution of multiple copy operations.
            - Requires appropriate access permissions on both source file and destination folder.

        Raises:
            Exception: Any API errors during metadata retrieval, deletion, or copy will be caught and returned in the response.
        """

        status = 'success'
        message = ''
        meta_data = {}
        new_file_id = ''

        # Step 1: Determine name to check
        name_to_check = new_name
        if not name_to_check:
            file_metadata = self.drive_service.files().get(fileId=file_id, fields='name').execute()
            name_to_check = file_metadata['name']

        # Step 2: Check if file already exists
        query = (
            f"'{new_folder_id}' in parents and "
            f"name='{name_to_check}' and trashed=false"
        )
        existing_files = self.drive_service.files().list(q=query, fields="files(id, modifiedTime)").execute().get('files', [])

        if existing_files:
            # Step 3: Compare modified times
            source_file_metadata = self.drive_service.files().get(
                fileId=file_id, fields='modifiedTime'
            ).execute()
            source_modified = source_file_metadata['modifiedTime']
            dest_modified = existing_files[0]['modifiedTime']
            src = isoparse(source_modified)
            dst = isoparse(dest_modified)
            if src <= dst:
                message = f"Skipping '{name_to_check}' — destination is newer or same."
                meta_data = {
                            'folder_id': new_folder_id,
                            'file_id': file_id,
                            'new_name': new_name
                        }
                return {
                    'status': 'skipped',
                    'response': {
                        'meta_data': meta_data,
                        'data': json.dumps({"records":[meta_data]}),
                        'message': message
                    }
                }

            # Step 4: Delete older destination file
            self.drive_service.files().delete(fileId=existing_files[0]['id']).execute()
            message = f"Overwriting '{name_to_check}' — source is newer."

        # Step 5: Prepare metadata and copy file
        copied_file_metadata = {
            'parents': [new_folder_id],
            'name': name_to_check
        }

        if new_name:
            copied_file_metadata['name'] = new_name

        def callback(request_id, response, exception):
            if exception:
                print(f"Batch error copying file: {exception}")
            else:
                print(f"Batch copied file '{response['name']}' to folder ID {new_folder_id}")

        if batch:
            request = self.drive_service.files().copy(
                fileId=file_id,
                body=copied_file_metadata,
                fields='id, name'
            )
            batch.add(request, callback=callback)
            message = f"Copy request added to batch for file '{name_to_check}'"
        else:
            copy_response = self.drive_service.files().copy(
                fileId=file_id,
                body=copied_file_metadata,
                fields='id, name'
            ).execute()
            new_file_id = copy_response['id']
            message = f"Copied file '{copy_response['name']}' (ID: {new_file_id})"

        meta_data = {
            'folder_id': new_folder_id,
            'file_id': new_file_id or file_id,
            'new_name': new_name or name_to_check
        }

        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records":[meta_data]}),
                'message': message
            }
        }

    def copy_gdrive_folder_recursive(self, source_folder_id=None, destination_parent_folder_id=None, new_folder_name=None, user_id=None):
        """
        Recursively copies a folder from one location in Google Drive to another, preserving structure and contents.

        This method:
        - Copies an entire folder (and its subfolders/files) from `source_folder_id` into a destination parent folder.
        - Skips copying if the destination contains a newer version of a file with the same name.
        - Uses batch operations for efficient copying of multiple files.
        - Reuses existing destination folders if one with the same name already exists.

        Parameters:
            source_folder_id (str): The ID of the Google Drive folder to copy.
            destination_parent_folder_id (str): The ID of the parent folder where the copied structure will be placed.
            new_folder_name (str, optional): Optional custom name for the new root folder. Defaults to the source folder's name.

        Returns:
            dict: A dictionary containing:
                - 'status' (str): 'success' if the operation completed without exception, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes `source_folder_id` and `new_folder_id`.
                    - 'data' (str): JSON-encoded metadata.
                    - 'message' (str): A detailed multi-line message log of all actions performed (copied, skipped, errors, etc.).

        Example:
            >>> copy_gdrive_folder_recursive(
                    source_folder_id='1SourceAbCdEf',
                    destination_parent_folder_id='1DestXyZ123',
                    new_folder_name='Backup_2025'
                )
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'source_folder_id': '1SourceAbCdEf',
                        'new_folder_id': '3NewBackupFolderId'
                    },
                    'data': '{"records": [{"source_folder_id": "1SourceAbCdEf", "new_folder_id": "3NewBackupFolderId"}]}',
                    'message': 'Created new folder "Backup_2025" with ID: ...\nCopied file "report.pdf"...'
                }
            }

        Features:
            - Automatically reuses existing folders if they match the `new_folder_name`.
            - Skips overwriting if destination file is newer or the same.
            - Deletes older destination files before replacing them.
            - Uses batch API for efficient file copying.
            - Fully recursive — all nested folders and files are handled.

        Notes:
            - Folder creation uses `mimeType='application/vnd.google-apps.folder'`.
            - Only non-trashed files/folders are considered.
            - Requires permission to read from the source and write to the destination.

        Raises:
            Exception: Any errors during metadata retrieval, folder creation, or copying are caught and returned in the `message`.

        Known Limitation:
            - The function assumes `drive_service` is accessible as a global or instance variable. Consider passing it explicitly.
            - Large folder trees might hit rate limits; exponential backoff or delay strategies can be added if needed.
        """

        status = 'success'
        messages = []
        new_folder_id = ''

        try:
            if not new_folder_name:
                source_folder = self.drive_service.files().get(fileId=source_folder_id, fields='name').execute()
                new_folder_name = source_folder['name']

            # Check if destination folder already exists
            query = (
                f"mimeType='application/vnd.google-apps.folder' and "
                f"'{destination_parent_folder_id}' in parents and "
                f"name='{new_folder_name}' and trashed=false"
            )
            results = self.drive_service.files().list(q=query, fields="files(id)").execute()
            existing = results.get('files', [])

            if existing:
                new_folder_id = existing[0]['id']
                messages.append(f"Using existing folder '{new_folder_name}' with ID: {new_folder_id}")
            else:
                new_folder_metadata = {
                    'name': new_folder_name,
                    'mimeType': 'application/vnd.google-apps.folder',
                    'parents': [destination_parent_folder_id]
                }
                new_folder = self.drive_service.files().create(body=new_folder_metadata, fields='id').execute()
                new_folder_id = new_folder['id']
                messages.append(f"Created new folder '{new_folder_name}' with ID: {new_folder_id}")

            # List contents of source folder
            query = f"'{source_folder_id}' in parents and trashed=false"
            response = self.drive_service.files().list(q=query, spaces='drive', fields="files(id, name, mimeType)").execute()
            items = response.get('files', [])

            batch = self.drive_service.new_batch_http_request()

            def callback(request_id, response, exception):
                nonlocal messages
                if exception:
                    messages.append(f"Error copying file: {exception}")
                else:
                    messages.append(f"Copied file '{response['name']}' to folder ID {new_folder_id}")

            for item in items:
                item_id = item['id']
                item_name = item['name']
                item_type = item['mimeType']

                if item_type == 'application/vnd.google-apps.folder':
                    messages.append(f"Recursively copying folder: {item_name}")
                    sub_result = self.copy_gdrive_folder_recursive(
                        source_folder_id=item_id,
                        destination_parent_folder_id=new_folder_id,
                        new_folder_name=item_name
                    )
                    messages.append(sub_result['response']['message'])
                else:
                    # Check for duplicate
                    file_query = (
                        f"'{new_folder_id}' in parents and "
                        f"name='{item_name}' and trashed=false"
                    )
                    existing_files = self.drive_service.files().list(
                        q=file_query, fields="files(id, modifiedTime)"
                    ).execute().get('files', [])

                    if existing_files:
                        source_file_metadata = self.drive_service.files().get(
                            fileId=item_id, fields='modifiedTime'
                        ).execute()
                        source_modified = source_file_metadata['modifiedTime']
                        dest_modified = existing_files[0]['modifiedTime']

                        if source_modified <= dest_modified:
                            messages.append(f"Skipping '{item_name}' — destination is newer or same.")
                            continue

                        self.drive_service.files().delete(fileId=existing_files[0]['id']).execute()
                        messages.append(f"Overwriting '{item_name}' — source is newer.")

                    copied_file_metadata = {
                        'parents': [new_folder_id],
                        'name': item_name
                    }

                    request = self.drive_service.files().copy(
                        fileId=item_id,
                        body=copied_file_metadata,
                        fields='id, name'
                    )
                    batch.add(request, callback=callback)

            batch.execute()

        except Exception as e:
            status = 'error'
            messages.append(f'Error: {str(e)}')

        meta_data = {
            'source_folder_id': source_folder_id,
            'new_folder_id': new_folder_id
        }

        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records":[meta_data]}),
                'message': "\n".join(messages)
            }
        }
      
    def parse_markdown(self, text=None, user_id=None):
        """Extended markdown parser for headings, bold, italic, italic+bold, and hyperlinks."""
        elements = []
        lines = text.split('\n')
        index = 1  # Google Docs indexes start at 1

        for line in lines:
            original_index = index
            content = line
            style = {}
            link = None

            # Check for Heading 2 first
            if line.startswith('## '):
                content = line[3:]
                style['bold'] = True
                style['fontSize'] = {'magnitude': 18, 'unit': 'PT'}

            # Check for Heading 1
            elif line.startswith('# '):
                content = line[2:]
                style['bold'] = True
                style['fontSize'] = {'magnitude': 24, 'unit': 'PT'}

            # Check for links [text](url)
            elif re.search(r'\[(.*?)\]\((.*?)\)', line):
                match = re.search(r'\[(.*?)\]\((.*?)\)', line)
                if match:
                    link_text = match.group(1)
                    url = match.group(2)
                    content = link_text
                    link = url

            # Check for bold text
            elif '**' in line:
                content = re.sub(r'\*\*(.*?)\*\*', r'\1', line)
                style['bold'] = True

            # Check for italic text
            elif '_' in line:
                content = re.sub(r'_(.*?)_', r'\1', line)
                style['italic'] = True

            # Always add a newline after each inserted content
            content += '\n'

            # Insert the text
            elements.append({
                'insertText': {
                    'location': {'index': index},
                    'text': content
                }
            })

            # Apply formatting if any
            if style:
                elements.append({
                    'updateTextStyle': {
                        'range': {
                            'startIndex': original_index,
                            'endIndex': original_index + len(content),
                        },
                        'textStyle': style,
                        'fields': ','.join(style.keys())
                    }
                })

            # Apply link if any
            if link:
                elements.append({
                    'updateTextStyle': {
                        'range': {
                            'startIndex': original_index,
                            'endIndex': original_index + len(content),
                        },
                        'textStyle': {
                            'link': {'url': link},
                            'underline': True,
                            'foregroundColor': {
                                'color': {
                                    'rgbColor': {
                                        'red': 0.0,
                                        'green': 0.0,
                                        'blue': 1.0
                                    }
                                }
                            }
                        },
                        'fields': 'link,underline,foregroundColor'
                    }
                })

            index += len(content)

        return elements

    def write_markdown_content(self, doc_id=None, markdown_text=None, user_id=None):
        status = 'success'
        message = ''
        meta_data = {}
        try:
            requests = self.parse_markdown(markdown_text)
            self.docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()
            message = f'Markdown content written into Doc ID: {doc_id}'
        except Exception as e:
            message = f'Error: {str(e)}'
            status = 'error'
        
        meta_data = {'doc_id':doc_id, 'message':message}
        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': meta_data,
                'message': message
            }
        }

    def extract_markdown_from_doc(self, doc_id=None, user_id=None):
        status = 'success'
        message = ''
        meta_data = {}
        try:
            doc = self.docs_service.documents().get(documentId=doc_id).execute()
            body = doc.get('body', {}).get('content', [])

            markdown_lines = []

            for element in body:
                paragraph = element.get('paragraph')
                if not paragraph:
                    continue

                elements = paragraph.get('elements', [])
                paragraph_style = paragraph.get('paragraphStyle', {})
                named_style = paragraph_style.get('namedStyleType')

                line = ""

                # Determine heading levels
                if named_style == 'HEADING_1':
                    line += "# "
                elif named_style == 'HEADING_2':
                    line += "## "

                # Check if it's a bullet list
                bullet = paragraph.get('bullet')
                if bullet:
                    line += "- "

                # Process text elements
                for elem in elements:
                    text_run = elem.get('textRun')
                    if not text_run:
                        continue

                    text = text_run.get('content', '').rstrip('\n')
                    text_style = text_run.get('textStyle', {})

                    # Apply formatting
                    if text_style.get('bold'):
                        text = f"**{text}**"
                    if text_style.get('italic'):
                        text = f"_{text}_"
                    if text_style.get('link'):
                        url = text_style['link'].get('url')
                        text = f"[{text}]({url})"

                    line += text

                markdown_lines.append(line)

            markdown_result = '\n'.join(markdown_lines)
            message = f'Document with id {doc_id} markdown returned.'
        except Exception as e:
            message = f'Error: {str(e)}'
            markdown_result =''
            status = 'error'

        meta_data = {
            'doc_id': doc_id
        }

        return {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': markdown_result,
                'message': message
            }
        }

    def fetch_file_from_gdrive(self, file_id=None, name=None, mime_type=None, user_id=None):
        """
        Streams a file from Google Drive into memory.
        Returns: {'status', 'data': BytesIO, 'mime_type', 'file_id', 'name', ...}
        """
        status = 'success'
        message = ''
        meta_data = {}
        file_buffer = io.BytesIO()

        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(file_buffer, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            file_buffer.seek(0)
            message = "File fetched successfully"
        except Exception as e:
            status = 'error'
            message = f"Error: {str(e)}"

        meta_data = {
            'file_id': file_id,
            'name': name,
            'mime_type': mime_type
        }
        return {
            'status': status,
            'data': file_buffer,
            'meta_data': meta_data,
            'message': message
        }

    def download_file_from_gdrive(
        self,
        file_id: str,
        name: str | None = None,
        mime_type: str | None = None,      # optional hint; if not given we'll fetch it
        download_path: str | Path = ".",   # directory to save into
        user_id: str | None = None,        # kept for your interface
        export_mime: str | None = None,    # optional: force export target for Google Docs/Sheets/Slides
    ):
        """
        Downloads a file from Google Drive and saves it locally.

        - For "regular" files (e.g., PDFs, images, zips), uses files().get_media(...)
        - For Google Docs/Sheets/Slides/etc (application/vnd.google-apps.*), uses files().export(...)

        Works on desktop, remote servers, and Colab.
        Returns a structured response with metadata.
        """
        status = "error"
        message = ""
        out_path_str = ""

        # ---- helpers -------------------------------------------------------------
        def _safe_filename(s: str) -> str:
            # Remove characters invalid on Windows/macOS/Linux paths
            s = re.sub(r'[\\/:*?"<>|]+', "_", s)
            s = s.strip().strip(".")
            return s or "download"

        # Reasonable export defaults for Google file types
        DEFAULT_EXPORTS = {
            "application/vnd.google-apps.document":  ("application/pdf", ".pdf"),
            "application/vnd.google-apps.spreadsheet": ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
            "application/vnd.google-apps.presentation": ("application/pdf", ".pdf"),
            "application/vnd.google-apps.drawing":   ("image/png", ".png"),
            "application/vnd.google-apps.script":    ("application/vnd.google-apps.script+json", ".json"),
            "application/vnd.google-apps.jam":       ("application/pdf", ".pdf"),
        }

        try:
            drive = self.drive_service  # must be initialized with OAuth creds
        except AttributeError:
            return {
                "status": "error",
                "response": {
                    "meta_data": {},
                    "data": {"records": []},
                    "message": "drive_service not set on this instance",
                },
                "message": "drive_service not set on this instance",
            }

        try:
            # ---- 1) Fetch metadata (to get true name & MIME type) ----------------
            meta = drive.files().get(
                fileId=file_id,
                fields="id, name, mimeType, size, modifiedTime",
                supportsAllDrives=True,
            ).execute()

            real_name = meta.get("name") or name or file_id
            real_mime = meta.get("mimeType") or mime_type or "application/octet-stream"

            # ---- 2) Decide download vs export ------------------------------------
            is_google_file = real_mime.startswith("application/vnd.google-apps.")
            if is_google_file:
                # pick export MIME & extension
                if export_mime:
                    export_ext = mimetypes.guess_extension(export_mime) or ""
                    exp_mime, exp_ext = export_mime, export_ext
                else:
                    exp_mime, exp_ext = DEFAULT_EXPORTS.get(
                        real_mime, ("application/pdf", ".pdf")
                    )
                download_kind = ("export", exp_mime, exp_ext)
            else:
                # binary download
                guessed_ext = mimetypes.guess_extension(real_mime) or ""
                download_kind = ("media", real_mime, guessed_ext)

            # ---- 3) Build output path -------------------------------------------
            # Use provided 'name' as base if given; otherwise Drive name
            base = Path(name).stem if name else Path(real_name).stem
            base = _safe_filename(base)

            # If the original name has a useful extension and we're doing media,
            # prefer it; otherwise use the guessed/export extension
            if not is_google_file and Path(real_name).suffix:
                suffix = Path(real_name).suffix
            else:
                suffix = download_kind[2] or ""

            # Ensure output directory
            out_dir = Path(download_path)
            if not out_dir.is_absolute():
                # relative paths resolve from current working directory (works on Colab/servers/desktop)
                out_dir = Path.cwd() / out_dir
            out_dir.mkdir(parents=True, exist_ok=True)

            # Make filename unique-ish by appending the id (your original behavior)
            filename = f"{base}{suffix}"
            out_path = out_dir / filename

            # ---- 4) Download -----------------------------------------------------
            kind, _, _ = download_kind
            if kind == "export":
                export_mime_final = download_kind[1]
                request = drive.files().export_media(fileId=file_id, mimeType=export_mime_final)
            else:
                request = drive.files().get_media(fileId=file_id)

            with open(out_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status_chunk, done = downloader.next_chunk()
                    # Optional: print progress somewhere (status_chunk.progress())
                    # print(f"Download {int(status_chunk.progress()*100)}%")

            out_path_str = str(out_path)
            status = "success"
            mode_str = "exported" if is_google_file else "downloaded"
            message = f"File {mode_str} successfully: {out_path_str}"

        except Exception as e:
            status = "error"
            message = f"Error: {e!s}"

        meta_data = {
            "file_id": file_id,
            "filepath": out_path_str,
            "source_name": meta.get("name") if "meta" in locals() else name,
            "source_mime_type": meta.get("mimeType") if "meta" in locals() else mime_type,
            "exported": True if "download_kind" in locals() and download_kind[0] == "export" else False,
            "export_mime": download_kind[1] if "download_kind" in locals() and download_kind[0] == "export" else None,
        }

        return {
            "status": status,
            "response": {
                "meta_data": meta_data,
                "data": {"records": [{"filepath": out_path_str, "mime_type": meta_data.get("source_mime_type")}]},
                "message": message,
            },
            "message": message,
        }


    def get_gdrive_csv_and_get_data(self, file_id=None, user_id=None):
        """
        Downloads a CSV file from Google Drive, reads its contents in memory, 
        and saves the data as a JSON string in the AiAssistantCallData model with metadata.

        Process Overview:
        - Fetches file metadata (name and MIME type) using Google Drive API.
        - Downloads the file content into memory without saving to disk.
        - Parses the CSV data into a list of dictionaries.
        - If records are found:
            - Extracts column names.
            - Creates a new AiAssistantCallData instance with:
                - A generated UUID (`data_id`)
                - JSON-dumped records
                - A description containing file name and file ID
        - If no records are found:
            - Returns a success message indicating no data was saved.
        - Handles exceptions gracefully, returning error status and message.

        Args:
            file_id (str): The Google Drive file ID of the CSV to download.

        Returns:
            dict: A structured response containing:
                - 'status' (str): 'success' or 'error'
                - 'response' (dict):
                    - 'data_id' (UUID or str): The UUID of the saved record (empty if none saved)
                    - 'data' (str): JSON string of the records (or empty list if none)
                    - 'meta_data' (dict): Contains 'data_id', 'message', and CSV field names
                    - 'message' (str): Informational message about the operation result
        """
        status = 'success'
        message = ''
        saved_instance = None
        records = []
        data='{"records":[]}'
        meta_data={}
        data_id=''
        columns=[]

        try:
            file_metadata = self.drive_service.files().get(
                fileId=file_id,
                fields='name, mimeType'
            ).execute()
            file_name = file_metadata.get('name')

            # Prepare an in-memory bytes buffer
            fh = io.BytesIO()

            # Download file into memory
            request = self.drive_service.files().get_media(fileId=file_id)
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            fh.seek(0)  # Reset pointer to the beginning of the file

            # Decode and read as CSV (assuming UTF-8)
            decoded_fh = io.TextIOWrapper(fh, encoding='utf-8')
            reader = csv.DictReader(decoded_fh)
            records = list(reader)

            # Save to DB
            if records:
                columns=records[0].keys()
                data_id=uuid.uuid4()
                description=f'File name:{file_name}, file_id:{file_id}'
                data=json.dumps({"records":records})
                message = f"Data for file_id={file_id} generated"
                meta_data={"file_id":file_id,"message":message, 'coloumns':', '.join(columns)}
            else:
                message = f"No data in File name:{file_name}, file_id:{file_id}"

            
        except Exception as e:
            status = 'error'
            message = f"Error: {str(e)}"

        return {
            'status': status,
            'response': {
                'data_id': data_id,
                'data': data,
                'meta_data':meta_data,
                'message': message
            },
            'message': message
        }


    ####################################################################
    #Google Sheets service
    ####################################################################

    def get_all_sheets_in_a_google_sheet(self, spreadsheet_id=None, user_id=None):
        """
        Fetch all worksheets (tabs) from a Google Sheets spreadsheet and return
        each sheet's rows as JSON-serializable records grouped by sheet title.

        This method:
        1) Lists sheet titles via the Sheets API (`spreadsheets().get`).
        2) For each title, reads values using `spreadsheets().values().get` with
            the sheet title as the range (letting the API auto-detect the used range).
        3) Treats the first row of each sheet as column headers and converts the
            remaining rows into a list of dict records using pandas.
        4) Packages results and basic metadata in a response dictionary.

        Args:
            spreadsheet_id (str | None): The spreadsheet ID (the long ID in the
                Google Sheets URL). Required for a successful call.
            user_id (str | int | None): Optional caller identifier included only
                for auditing/telemetry purposes. Not used by the current
                implementation.

        Returns:
            dict: A dictionary with the shape:
                {
                    "status": "<string>",
                    "response": {
                        "meta_data": {
                            "spreadsheet_id": "<the input spreadsheet_id>",
                            "message": "<aggregated info/warnings or error text>"
                        },
                        "data": "<JSON string of the form: {\"records\": [ {<sheet_title>: [ {...}, ... ]} ]}>"
                    },
                    "message": "<same as meta_data.message>"
                }

                Where:
                - The top-level `status` indicates outcome. (Note: in the current
                    implementation it is initialized as "error" and only changed on
                    exception handling; adjust if you'd like to indicate success.)
                - `response.meta_data.message` aggregates per-sheet notes (e.g.,
                    skipped empty sheets) or error details.
                - `response.data` is a JSON string containing a list with one
                    item: a mapping from each sheet title to a list of row records.

        Behavior & Assumptions:
            - Empty sheets: If a sheet has no values, it is skipped and a note is
            appended to the message.
            - Headers: The first row of each sheet is assumed to contain column
            names. Subsequent rows are mapped into dicts using those headers.
            - Types: All values are returned as strings by the Sheets API; no type
            coercion is performed beyond pandas' construction of the DataFrame.
            - Ordering: Rows retain the order returned by the API.
            - Error handling: Any exception is caught; the error message is stored
            in `message`, and `status` is set/left as "error". No exceptions are
            raised.

        Dependencies:
            - `self.sheets_service`: An authenticated Google Sheets API v4 client
            (e.g., from `googleapiclient.discovery.build("sheets", "v4", ...)`).
            - `pandas` as `pd`.
            - `json` for serialization.

        API Usage & Quotas:
            - Performs 1 + N API calls: one to list sheets and one per sheet to read
            values. Large spreadsheets or many sheets may approach quota limits.

        Example:
            >>> out = obj.get_all_sheets_in_a_google_sheet("1AbCDefGhIjKLMNOPqRstuVwXyZ")
            >>> meta = out["response"]["meta_data"]
            >>> meta["spreadsheet_id"]
            '1AbCDefGhIjKLMNOPqRstuVwXyZ'
            >>> records = json.loads(out["response"]["data"])["records"][0]
            >>> list(records.keys())  # sheet titles
            ['Sheet1', 'Summary', 'Data_2025']

        Notes:
            - Column header quality matters. Duplicate or empty headers can produce
            unexpected keys in the record dicts. Consider normalizing headers
            before converting to records if needed.
            - If you want `status` to reflect success, set it to "success" when the
            try-block completes without exceptions.
        """
        status='error'
        meta_data ={}
        data_json = json.dumps([])
        message = ''
        sheet = {}
        try:        
            # Get all sheet names
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_titles = [s['properties']['title'] for s in metadata['sheets']]
        

            for title in sheet_titles:
                # ✅ Don't limit range — let Sheets API auto-detect
                result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range=title  
                ).execute()

                values = result.get('values', [])
                if not values:
                    messahe+=f"\n Sheet '{title}' is empty."
                    continue

                df = pd.DataFrame(values[1:], columns=values[0])
                sheet[title] = df.to_dict(orient='records')
                message+= f"\n Returned sheet: {title} → df_{title}"
        except Exception as e:
            message = f'Error: {str(e)}'
            status = 'error'

        meta_data = {'spreadsheet_id':spreadsheet_id, 'message':message}
    
        response = {"meta_data": meta_data, "data":json.dumps({"records":[sheet]})}
        response = {
            'status': status,
            'response':response, 
            'message':message
        } 
        return response

    def add_dataframe_as_new_sheet(self, spreadsheet_id=None, data=None, new_sheet_name=None, user_id=None):
        """
        Create a new tab in a Google Sheets spreadsheet and write a table of values
        (derived from a pandas DataFrame) starting at cell A1, with the first row
        containing column headers.

        Workflow:
        1) Convert `data` to a pandas DataFrame (`pd.DataFrame(data)`).
        2) Use the Sheets `spreadsheets().batchUpdate(..., addSheet=...)` request to
            create a new worksheet (tab) named `new_sheet_name`.
        3) Write the DataFrame's headers + values to the new tab via
            `spreadsheets().values().update` with `valueInputOption='RAW'`.

        Args:
            spreadsheet_id (str | None): The Google Sheets spreadsheet ID (the long
                ID in the URL). Required for a successful call.
            data (Any): Tabular data convertible to a pandas DataFrame. Common forms:
                - pandas.DataFrame
                - list[dict]  (records)
                - dict[list]  (columns)
                - list[list]  (rows)
                The first DataFrame row is written as headers, remaining rows as values.
            new_sheet_name (str | None): Name for the new tab. Must be unique within
                the spreadsheet; if a sheet with the same name already exists, the
                API returns an error.
            user_id (str | int | None): Optional caller identifier for logging or
                telemetry; not used by the implementation.

        Returns:
            dict: A dictionary shaped as:
                {
                "status": "success" | "error",
                "response": {
                    "meta_data": {
                    "spreadsheet_id": "<input spreadsheet_id>",
                    "new_sheet_name": "<input new_sheet_name>",
                    "message": "<operation log or error details>"
                    },
                    "data": { ...same as meta_data... },
                    "message": "<same text as meta_data.message>"
                }
                }

                Notes on fields:
                - "status" reflects the overall outcome; on success it is "success",
                    otherwise "error".
                - "response.meta_data.message" aggregates step-by-step notes
                    (e.g., tab created, data written) or the error text if an exception occurs.

        Behavior & Assumptions:
            - Sheet creation is separate from data write; if the write step fails,
            the new (empty) tab will remain unless cleaned up explicitly.
            - Values are written with `valueInputOption='RAW'`, meaning Google Sheets
            does not parse numbers/dates/formulas. Use 'USER_ENTERED' if you want
            Sheets to apply its own parsing/formatting.
            - Headers: DataFrame column labels are written as the first row. Ensure
            they are strings or convertible to strings.
            - Types: Values are serialized as basic Python scalars (e.g., numbers,
            strings); DataFrame objects like Timestamp/NA are converted by
            `.values.tolist()`—consider normalizing before writing.
            - Name collisions: If `new_sheet_name` already exists or is invalid, the
            addSheet request will raise an API error that is caught and reported.
            - Error handling: All exceptions are caught; no exceptions propagate.

        Dependencies:
            - `self.sheets_service`: An authenticated Google Sheets API v4 client
            (e.g., from `googleapiclient.discovery.build("sheets","v4",...)`).
            - `pandas` as `pd`.

        Quotas & Size:
            - Performs two API calls: one `batchUpdate` (addSheet) and one `values.update`.
            Large tables are subject to Google Sheets size and quota limits.

        Example:
            >>> payload = [
            ...   {"name": "Alice", "score": 95},
            ...   {"name": "Bob",   "score": 88},
            ... ]
            >>> out = obj.add_dataframe_as_new_sheet(
            ...     spreadsheet_id="1AbCDefGhIjKLMNOPqRstuVwXyZ",
            ...     data=payload,
            ...     new_sheet_name="Results_2025_09_23",
            ... )
            >>> out["status"]
            'success'
            >>> out["response"]["meta_data"]["message"]
            "\\n Created new tab: Results_2025_09_23\\n Data written to 'Results_2025_09_23' tab."

        Notes:
            - If you want idempotency, consider checking for an existing sheet with
            the same name and either deleting/reusing it or generating a unique name.
            - To preserve cell formatting or formulas, you can switch to
            batchUpdate with `updateCells` or set `valueInputOption='USER_ENTERED'`
            and provide formula strings (e.g., '=SUM(A2:A10)').
        """
        # ... existing implementation ...

        status='error'
        meta_data ={}
        data_json = json.dumps([])
        message = ''

        try:
            df = pd.DataFrame(data)
            # Step 1: Create the new sheet/tab
            add_sheet_request = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': new_sheet_name
                        }
                    }
                }]
            }

            self.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=add_sheet_request
            ).execute()

            message += f"\n Created new tab: {new_sheet_name}"

            # Step 2: Prepare data: columns + values
            values = [df.columns.tolist()] + df.values.tolist()

            # Step 3: Write values to the new sheet
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{new_sheet_name}!A1",
                valueInputOption='RAW',
                body={'values': values}
            ).execute()

            message += f"\n Data written to '{new_sheet_name}' tab."
            status='success'
        except Exception as e:
            message = f'Error: {str(e)}'
                
        meta_data = {'spreadsheet_id':spreadsheet_id, 'new_sheet_name':new_sheet_name,'message':message}
        response = {"meta_data": meta_data, "data":meta_data, "message":message}
        response = {
            'status': status,
            'response':response
        }

        return response 

    ####################################################################
    #Google Calendar
    ####################################################################

    def create_google_calendar_event(
        self,
        summary='',
        start_time=None,
        end_time=None,
        description='',
        location='',
        timezone='UTC',
        calendar_id='primary',
        attendees_emails=None,
        use_google_meet=False,
        custom_join_link=None,
        user_id=None 
    ):
        """
        Creates an event in Google Calendar with optional attendees, location, and video conferencing.

        This method uses the Google Calendar API to create an event in the specified calendar. It supports:
        - Attendee invitations
        - Google Meet link generation
        - Custom join/location links
        - Validation of ISO 8601 datetime inputs

        Parameters:
            summary (str): Title of the event.
            start_time (str): ISO 8601 formatted start datetime (e.g., '2025-06-01T09:00:00').
            end_time (str): ISO 8601 formatted end datetime.
            description (str, optional): Event description.
            location (str, optional): Event location (can be overridden by `custom_join_link`).
            timezone (str, optional): Timezone of the event (default is 'UTC').
            calendar_id (str, optional): Calendar ID to add the event to (default is 'primary').
            attendees_emails (list[str], optional): List of attendee email addresses to invite.
            use_google_meet (bool, optional): If True, creates a Google Meet link for the event.
            custom_join_link (str, optional): Custom link to join the event. Appended to the description and used as location.

        Returns:
            dict: A dictionary containing:
                - 'status' (str): 'success' if the event was created, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes event summary and times.
                    - 'data' (dict): Raw Google Calendar event resource (empty if creation failed).
                    - 'message' (str): Result message with event or error info.

        Example:
            >>> create_google_calendar_event(
                    summary="Team Sync",
                    start_time="2025-06-01T10:00:00",
                    end_time="2025-06-01T11:00:00",
                    attendees_emails=["alice@example.com", "bob@example.com"],
                    use_google_meet=True
                )
            {
                "status": "success",
                "response": {
                    "meta_data": {
                        "event_summary": "Team Sync",
                        "start_time": "2025-06-01T10:00:00",
                        "end_time": "2025-06-01T11:00:00"
                    },
                    "data": { ... },  # Full event resource
                    "message": "Event created: https://calendar.google.com/calendar/event?eid=...\nGoogle Meet Link: https://meet.google.com/..."
                }
            }

        Notes:
            - Validates start and end time formats using `dateutil.parser.isoparse`.
            - Automatically sends invites to attendees if specified.
            - `conferenceDataVersion=1` is required for Meet links.
            - Use `custom_join_link` for third-party conferencing tools (e.g., Zoom, Teams).

        Raises:
            ValueError: If start_time or end_time is not in valid ISO 8601 format.
            Exception: For any Google Calendar API errors.
        """

        def validate_iso8601(dt_str, field_name):
            if dt_str is None:
                return None
            try:
                dt = isoparse(dt_str)
                return dt.isoformat()  # ensures normalized ISO 8601 string
            except Exception as e:
                raise ValueError(
                    f"Invalid {field_name}: '{dt_str}'. "
                    "Must be ISO 8601, e.g., '2025-06-01T09:00:00'."
                ) from e

        status = 'error'
        meta_data = {}
        message = ''
        created_event = {}

        try:
            # ✅ Validate ISO 8601 format
            start_time = validate_iso8601(start_time, "start_time")
            end_time = validate_iso8601(end_time, "end_time")

            # Ensure chronological order
            if start_time and end_time and isoparse(end_time) <= isoparse(start_time):
                message = "end_time must be after start_time"
                status = 'error'

            elif start_time and end_time:
                event = {
                    'summary': summary,
                    'description': description,
                    'start': {
                        'dateTime': start_time,
                        'timeZone': timezone,
                    },
                    'end': {
                        'dateTime': end_time,
                        'timeZone': timezone,
                    },
                    'reminders': {
                        'useDefault': True,
                    },
                }

                if location:
                    event['location'] = location

                if attendees_emails:
                    event['attendees'] = [{'email': email} for email in attendees_emails]

                if custom_join_link:
                    event['description'] += f"\n\nJoin via: {custom_join_link}"
                    event['location'] = custom_join_link

                if use_google_meet:
                    event['conferenceData'] = {
                        'createRequest': {
                            'requestId': f"meet-{uuid.uuid4().hex}",
                            'conferenceSolutionKey': {'type': 'hangoutsMeet'}
                        }
                    }

                insert_kwargs = {
                    "calendarId": calendar_id,
                    "body": event,
                    "sendUpdates": "all",
                }

                if use_google_meet:
                    insert_kwargs["conferenceDataVersion"] = 1
                created = self.calendar_service.events().insert(**insert_kwargs).execute()
                meet = None
                for ep in created.get("conferenceData", {}).get("entryPoints", []):
                    if ep.get("entryPointType") == "video":
                        meet = ep.get("uri"); break


                created_event = self.calendar_service.events().insert(**insert_kwargs).execute()

                message = f"\nEvent created: {created_event.get('htmlLink')}"
                if use_google_meet:
                    meet_link = created_event.get('conferenceData', {}).get('entryPoints', [{}])[0].get('uri')
                    message += f"\nGoogle Meet Link: {meet_link}"

                status = 'success'
            else:
                message = "start_time and end_time must be provided."
                status = 'error'

        except ValueError as ve:
            message = f"Invalid datetime format: {ve}"
        except Exception as e:
            message = f'Error: {str(e)}'

        meta_data = {
            'event_summary': summary,
            'start_time': start_time,
            'end_time': end_time
        }

        response = {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records":[created_event]}),
                'message': message
            },
            'message': message
        }

        return response
      
    def get_google_calendar_events_between(
        self,
        start_time=None,
        end_time=None,
        calendar_id='primary',
        timezone='UTC',
        max_results=100,
        user_id=None
    ):
        """
        Retrieves Google Calendar events between two datetimes.

        This method fetches a list of events from a specified Google Calendar that fall within the given
        time range. The results are returned in chronological order and limited by `max_results`.

        Parameters:
            start_time (str): ISO 8601 formatted start datetime (e.g., '2025-06-01T00:00:00Z').
            end_time (str): ISO 8601 formatted end datetime (e.g., '2025-06-02T00:00:00Z').
            calendar_id (str, optional): Google Calendar ID (defaults to 'primary' for the authenticated user).
            timezone (str, optional): Timezone to apply if input datetimes are naive (default: 'UTC').
            max_results (int, optional): Maximum number of events to return (default: 100).

        Returns:
            dict: A dictionary with:
                - 'status' (str): 'success' if retrieval was successful, otherwise 'error'.
                - 'response' (dict):
                    - 'meta_data' (dict): Includes `calendar_id`, `start_time`, and `end_time`.
                    - 'data' (list[dict]): List of calendar events, each with:
                        - id (str): Event ID.
                        - summary (str): Event title.
                        - start (dict): Event start datetime object.
                        - end (dict): Event end datetime object.
                        - location (str): Event location (if any).
                        - description (str): Event description (if any).
                        - htmlLink (str): URL to view the event in Google Calendar.
                    - 'message' (str): Status message, including the number of events retrieved.

        Example:
            >>> get_google_calendar_events_between(
                    start_time='2025-06-01T00:00:00Z',
                    end_time='2025-06-07T23:59:59Z'
                )
            {
                'status': 'success',
                'response': {
                    'meta_data': {
                        'start_time': '2025-06-01T00:00:00Z',
                        'end_time': '2025-06-07T23:59:59Z',
                        'calendar_id': 'primary'
                    },
                    'data': [
                        {
                            'id': 'abc123',
                            'summary': 'Team Standup',
                            'start': {'dateTime': '2025-06-01T09:00:00Z'},
                            'end': {'dateTime': '2025-06-01T09:30:00Z'},
                            ...
                        },
                        ...
                    ],
                    'message': 'Retrieved 5 event(s).'
                }
            }

        Notes:
            - The method ensures that datetime strings are in ISO 8601 format and includes timezone info.
            - If the `start_time` or `end_time` is naive (no timezone), the provided `timezone` is applied.
            - Uses `calendar_service.events().list()` with `singleEvents=True` and `orderBy='startTime'`.

        Raises:
            ValueError: If the datetime format is invalid.
            Exception: For any Google Calendar API errors.
        """

        status = 'error'
        events_data = []
        message = ''
        meta_data = {}

        try:
            # ✅ Validate ISO 8601 format
            start_dt = isoparse(start_time)
            if not start_dt.tzinfo:
                start_dt = start_dt.replace(tzinfo=UTC)
            start_time = start_dt.isoformat()

            end_dt = isoparse(end_time)
            if not end_dt.tzinfo:
                end_dt = end_dt.replace(tzinfo=UTC)
            end_time = end_dt.isoformat()


            events_result = self.calendar_service.events().list(
                calendarId=calendar_id,
                timeMin=start_time,
                timeMax=end_time,
                singleEvents=True,
                orderBy='startTime',
                maxResults=max_results
            ).execute()

            events = events_result.get('items', [])
            status = 'success'

            for event in events:
                events_data.append({
                    'id': event.get('id'),
                    'summary': event.get('summary'),
                    'start': event.get('start'),
                    'end': event.get('end'),
                    'location': event.get('location'),
                    'description': event.get('description'),
                    'htmlLink': event.get('htmlLink')
                })

            message = f"Retrieved {len(events_data)} event(s)."

        except ValueError as ve:
            message = f"Invalid datetime format: {ve}"
        except Exception as e:
            message = f"Error fetching events: {str(e)}"

        meta_data = {
            'start_time': start_time,
            'end_time': end_time,
            'calendar_id': calendar_id
        }

        response = {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': json.dumps({"records":events_data}),
                'message': message
            }
        }

        return response

    def delete_google_calendar_events_by_ids(
        self,
        event_ids,
        calendar_id='primary',
        user_id=None
    ):
        status = 'partial'  # Default: assume some deletions may fail
        deleted = []
        failed = []

        for event_id in event_ids:
            try:
                self.calendar_service.events().delete(
                    calendarId=calendar_id,
                    eventId=event_id
                ).execute()
                deleted.append(event_id)
            except Exception as e:
                failed.append({'event_id': event_id, 'error': str(e)})

        if deleted and not failed:
            status = 'success'
        elif not deleted and failed:
            status = 'error'

        response = {
            'status': status,
            'response': {
                'meta_data': {
                    'calendar_id': calendar_id,
                    'attempted': len(event_ids),
                    'deleted': len(deleted),
                    'failed': len(failed)
                },
                'data': {
                    'deleted_event_ids': deleted,
                    'failed_deletions': failed
                },
                'message': f"Deleted {len(deleted)} event(s), failed {len(failed)}."
            }
        }

        return response

    ###############################################################
    #Tasks
    ###############################################################

    def create_google_task(
        self,
        title='',
        notes='',
        due=None,
        tasklist_id='@default',
        user_id=None
    ):
        """
        Create a task in Google Tasks and return a structured response containing
        the created task resource, metadata, and status.

        Workflow:
        1) Build a Tasks API task body with `title` and `notes`.
        2) If `due` is provided, parse it using `dateutil.parser.isoparse` and
            format it as an RFC3339 timestamp string for the Google Tasks API.
        3) Insert the task into the specified task list via
            `tasks().insert(tasklist=..., body=...)`.
        4) Capture success or error details in a standardized response dict.

        Args:
            title (str, optional):
                Task title. Can be empty, but Google Tasks may surface untitled tasks
                with a blank name in the UI. Defaults to "".
            notes (str, optional):
                Freeform notes/description for the task. Defaults to "".
            due (str | None, optional):
                Due date/time in a string that `dateutil.parser.isoparse` can parse
                (e.g., "2025-09-24", "2025-09-24T15:30:00Z", "2025-09-24T10:30:00-05:00").
                If provided, it is formatted to RFC3339 for the API.
                Note: In the current implementation, the formatted string is forced
                to end with "Z" (UTC designator) without converting time zones; ensure
                your `due` is already in UTC if you care about exact UTC time.
            tasklist_id (str, optional):
                The Tasklist ID to insert into. Use "@default" for the user's
                primary list. Defaults to "@default".
            user_id (str | int | None, optional):
                Optional caller identifier for logging/telemetry; not used by the
                current implementation.

        Returns:
            dict: A response dictionary of the form:
                {
                "status": "success" | "error",
                "response": {
                    "meta_data": {
                    "title": "<input title>",
                    "due": "<original due string or None>",
                    "tasklist_id": "<input tasklist_id>"
                    },
                    "data": { ...created Google Tasks resource... },
                    "message": "<human-readable status message>"
                }
                }

                Notes:
                - On success, `data` contains the created task resource returned by
                    the API (including fields like 'id', 'title', 'status', 'due', etc.).
                - On error, `data` is an empty dict and `message` contains details.

        Behavior & Assumptions:
            - Date handling:
                * `due` is parsed with `isoparse`. If you pass a date-only string
                (e.g., "2025-09-24"), it will be interpreted as midnight.
                * The current implementation formats the due datetime as
                `YYYY-MM-DDTHH:MM:SS.000Z` regardless of the input timezone. If
                your input is timezone-aware and not UTC, consider converting to
                UTC before formatting to avoid an unintended shift.
            - Error handling:
                * `ValueError` from parsing `due` is caught separately to provide a
                clearer message.
                * Any other exception is caught and reported in the response message.
                * No exceptions are raised to the caller.
            - Idempotency:
                * Multiple calls create multiple tasks; there is no deduplication.

        Dependencies:
            - `self.tasks_service`: An authenticated Google Tasks API client
            (e.g., from `googleapiclient.discovery.build("tasks", "v1", ...)`).
            - `dateutil.parser.isoparse` for robust ISO-8601 parsing.

        Quotas:
            - Performs one API call (`tasks.insert`). Subject to Google Tasks quota
            and per-user limits.

        Example:
            >>> out = obj.create_google_task(
            ...     title="Prepare Q4 report",
            ...     notes="Gather metrics and draft executive summary.",
            ...     due="2025-09-30T17:00:00Z",   # already UTC
            ...     tasklist_id="@default",
            ... )
            >>> out["status"]
            'success'
            >>> out["response"]["data"]["title"]
            'Prepare Q4 report'

        Notes:
            - If you want robust, timezone-safe handling, convert parsed datetimes
            to UTC explicitly before formatting (e.g., `dt.astimezone(timezone.utc)`).
            - Google Tasks treats due times differently across clients; some UIs
            emphasize the date more than the exact time.
        """
        # ... existing implementation ...    
        status = 'error'
        message = ''
        created_task = {}
        meta_data = {}

        try:
            task_body = {
                'title': title,
                'notes': notes,
            }

            if due:
                # Validate and format due date
                due_dt = isoparse(due)
                # Format for RFC3339 (required by Google Tasks)
                task_body['due'] = due_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')

            created_task = self.tasks_service.tasks().insert(
                tasklist=tasklist_id,
                body=task_body
            ).execute()

            status = 'success'
            message = f"Task created: {created_task.get('title')}"

        except ValueError as ve:
            message = f"Invalid datetime format: {ve}"
        except Exception as e:
            message = f"Error creating task: {str(e)}"

        meta_data = {
            'title': title,
            'due': due,
            'tasklist_id': tasklist_id
        }

        response = {
            'status': status,
            'response': {
                'meta_data': meta_data,
                'data': created_task,
                'message': message
            }
        }

        return response

    ######################################################
    #Google forms
    ######################################################

    def enable_google_form_email_collection(self, form_id: str, mode: str = "RESPONDER_INPUT"):
        allowed = {"VERIFIED", "RESPONDER_INPUT", "DO_NOT_COLLECT"}
        if mode not in allowed:
            raise ValueError(f"mode must be one of {sorted(allowed)}")

        body = {
            "includeFormInResponse": True,  # so you can read back settings
            "requests": [{
                "updateSettings": {
                    "settings": {"emailCollectionType": mode},
                    "updateMask": "emailCollectionType",
                }
            }]
        }
        return self.forms_service.forms().batchUpdate(formId=form_id, body=body).execute()

    def create_google_form(self, title="New Form Title", email_collection_mode="RESPONDER_INPUT", user_id=None):
        """
        Create a new Google Form, then set its email-collection behavior, and return
        a structured response with identifiers and the shareable responder URL.

        Email collection modes:
            - "VERIFIED": Require Google sign-in and automatically capture the
            signed-in user’s email. Responses include `respondentEmail`.
            - "RESPONDER_INPUT": Add a built-in email field that respondents type
            into (no sign-in required). Responses include `respondentEmail` with
            whatever address was entered (subject to Google’s email validation).
            - "DO_NOT_COLLECT": Do not collect email; responses will have no
            `respondentEmail` (anonymous submissions).

        Implementation details:
            - Step 1: Calls `forms().create(...)` to create an empty form shell.
            - Step 2: Immediately calls `enable_google_form_email_collection(form_id, mode=...)`
            which performs a `forms().batchUpdate(..., updateSettings)` to set
            `Form.settings.emailCollectionType` to the requested mode. (Settings
            cannot be set at creation time.)

        Args:
            title (str, optional):
                Form title shown to respondents and used as the document title. Defaults to "New Form Title".
            email_collection_mode (str, optional):
                One of {"VERIFIED", "RESPONDER_INPUT", "DO_NOT_COLLECT"}.
                Defaults to "RESPONDER_INPUT".
            user_id (str | int | None, optional):
                Optional caller identifier for logging/telemetry; not used by this implementation.

        Returns:
            dict: {
                "status": "success" | "error",
                "message": "<human-readable message>",
                "response": {
                    "data": "<JSON string with {'records': [{'form_id', 'responderUri', 'editorUri'}]}>"
                },
                "responderUri": "<URL respondents can use to fill the form>" | None,
                "editorUri": "<editor/document URL if present in response>" | None,
                "meta_data": <raw create response>
            }

            Notes:
                - The Forms API reliably returns `formId` and `responderUri`.
                An explicit editor URL may not be included; consider using the
                Drive API to fetch a web view link if needed.

        Behavior & assumptions:
            - Creates an empty form (no items). Add questions later via
            `forms().batchUpdate` (e.g., `createItem` requests).
            - If you need mixed anonymous and identified responses, the API setting
            is global per form. Use "DO_NOT_COLLECT" plus your own optional
            email question, or keep "RESPONDER_INPUT" and mark that email field
            as optional in your UX copy.
            - Exceptions are caught and summarized; no exceptions propagate.

        Dependencies:
            - `self.forms_service`: Authenticated Google Forms API client.
            - OAuth scopes: include `https://www.googleapis.com/auth/forms.body`
            (and Drive scopes if you later query Drive for editor links).

        Example:
            >>> out = obj.create_google_form(
            ...     title="Event Feedback",
            ...     email_collection_mode="VERIFIED",
            ... )
            >>> out["status"]
            'success'
            >>> json.loads(out["response"]["data"])["records"][0]["form_id"]
            'abc123DEF456ghi789'
            >>> out["responderUri"]
            'https://docs.google.com/forms/d/e/.../viewform'
        """
        description=''
        new_form = {
            "info": {
                "title": title,
                "documentTitle": title,
                "description": description
            }
        }

        try:
            created_form = self.forms_service.forms().create(body=new_form).execute()
            form_id=created_form.get('formId')
            self.enable_google_form_email_collection(form_id, mode=email_collection_mode)

            return {
                "status": "success",
                "message": f"Form: form_id = {created_form.get('formId')}, URL: {created_form.get('responderUri')} Created",
                "response":{
                    'data':json.dumps({'records':[{
                            "form_id":created_form.get('formId'),
                            "responderUri": created_form.get('responderUri'),
                            "editorUri": created_form.get('documentUri')
                        }
                    ]})
                },
                'meta_data':created_form,
                "responderUri": created_form.get('responderUri'),
                "editorUri": created_form.get('documentUri')
            }
        except Exception as e:
            return {"status": "error", "response":{"error":str(e)}, "message": str(e)}

    def add_questions_to_google_form(self, form_id, questions, user_id=None):
        """
        Add one or more items (e.g., questions) to an existing Google Form using
        the Forms API `batchUpdate` endpoint.

        Workflow:
        1) Accept a list of batchUpdate request objects in `questions`.
        2) Call `forms().batchUpdate(formId=form_id, body={"requests": questions})`.
        3) Return a structured response with the raw API result on success, or an
            error message on failure.

        Args:
            form_id (str):
                The target Google Form ID (from the Forms API or the form's URL).
            questions (list[dict]):
                A list of Forms API batchUpdate request objects. Each entry should
                follow the Google Forms API schema (e.g., a `createItem` request).
                This function does **not** validate the structure beyond passing it
                through to the API.
            user_id (str | int | None, optional):
                Optional caller identifier for logging/telemetry; not used by the
                current implementation.

        Returns:
            dict: A dictionary shaped like:
                {
                "status": "success" | "error",
                "message": "<human-readable status message>",
                "response": <raw batchUpdate response dict or {} on error>
                }

                Notes:
                - On success, `response` contains the Forms API `batchUpdate`
                    response (e.g., a `replies` array).
                - On error, `response` is `{}` and `message` contains details.

        Behavior & Assumptions:
            - This method forwards your `requests` to the API exactly as provided.
            If a request is malformed (e.g., invalid field names or item shapes),
            the API will raise an error that is caught and returned.
            - No local retries or partial-success handling are performed.
            - The function does not currently request the updated form object in the
            response. If you want the updated form returned by the API, consider
            extending the `body` with:
                {"requests": [...],
                "includeFormInResponse": True,
                "responseMask": {"fields": "*"}}

        Dependencies:
            - `self.forms_service`: An authenticated Google Forms API v1 client
            (e.g., from `googleapiclient.discovery.build("forms", "v1", ...)`).

        Quotas:
            - One `forms.batchUpdate` call per invocation; subject to project/user
            quota and rate limits.

        Examples:
            Basic multiple-choice question at index 0:
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
                                "options": [{"value": "Red"},
                                            {"value": "Blue"},
                                            {"value": "Green"}],
                                "shuffle": False
                            }
                            }
                        }
                        },
                        "location": {"index": 0}
                    }
                    }
                ]
                out = obj.add_questions_to_google_form(form_id="abc123", questions=requests)
                assert out["status"] == "success"

            Short-answer question appended to the end (use a large index):
                requests = [
                    {
                    "createItem": {
                        "item": {
                        "title": "What did you like most?",
                        "questionItem": {
                            "question": {
                            "required": False,
                            "textQuestion": {"paragraph": True}
                            }
                        }
                        },
                        "location": {"index": 9999}
                    }
                    }
                ]
                out = obj.add_questions_to_google_form("abc123", requests)

        Notes:
            - Common request types include `createItem`, `moveItem`, `updateItem`,
            and `deleteItem`. For new questions, use `createItem` with an `item`
            that wraps a `questionItem`. For choices, use `choiceQuestion`; for
            free text, use `textQuestion`.
            - If you need to localize or set validation rules, include the relevant
            subfields in the `question` object (e.g., `grading`, `rowQuestion`,
            `scaleQuestion`, `validation`).
        """
        

        try:
            result = self.forms_service.forms().batchUpdate(
                formId=form_id,
                body={"requests": questions}
            ).execute()

            return {
                "status": "success",
                "message": f"Questions added to form {form_id}",
                "response": {
                    'meta_data':result,
                    'data':json.dumps({'records':[result]})
                }
            }

        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "response": {}
            }

    def get_google_form_responses(self, form_id, use_ids=False, as_lists=False, user_id=None):
        """
        Retrieve responses for a Google Form and return them as JSON-serializable
        records, using question titles as column keys.

        Workflow:
        1) Fetch the form definition via `forms().get(formId=...)` to build a
            mapping of `questionId -> question title` for items that are questions.
        2) Fetch submitted responses via `forms().responses().list(formId=...)`.
        3) For each response, create a record containing:
            - "responseId"
            - "createTime" (RFC3339 string from the API)
            - One key per question, using the question title (falls back to the
                questionId if the title can’t be found).
            The current implementation extracts only **text answers** and, when
            multiple text answers exist for a single question, keeps the **first**.

        Args:
            form_id (str):
                The Google Form ID (from the Forms API or the form's URL).
            user_id (str | int | None, optional):
                Optional caller identifier for logging/telemetry; not used by the
                current implementation.

        Returns:
            dict: A dictionary shaped like:
                {
                "status": "success" | "error",
                "message": "Retrieved N response(s)." | "<error text>",
                "response": {
                    "data": "<JSON string of the form {'records': [...]}>"
                }
                }

        """
        try:
            # 1) Build questionId -> title map in form order
            form_def = self.forms_service.forms().get(formId=form_id).execute()
            qmap = {}
            for item in form_def.get("items", []):
                qi = item.get("questionItem", {})
                q = qi.get("question") if qi else None
                if not q:
                    continue
                qid = q.get("questionId")
                title = (item.get("title") or qid).strip()
                qmap[qid] = title

            # 2) Fetch ALL responses (paginate)
            records_raw = []
            page_token = None
            while True:
                resp = self.forms_service.forms().responses().list(
                    formId=form_id, pageToken=page_token
                ).execute()
                records_raw.extend(resp.get("responses", []))
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

            # Expand map with QIDs that exist only in historical responses (deleted/renamed questions)
            for r in records_raw:
                for qid in (r.get("answers") or {}).keys():
                    qmap.setdefault(qid, qid)

            # 3) Resolve display keys (titles) and avoid collisions
            resolved_key_for_qid = {}
            seen = set()
            for qid, title in qmap.items():
                key = qid if use_ids else title
                if key in seen and not use_ids:
                    # Disambiguate duplicate titles
                    i = 2
                    while f"{title} ({i})" in seen:
                        i += 1
                    key = f"{title} ({i})"
                resolved_key_for_qid[qid] = key
                seen.add(key)

            # Column order: metadata then questions in form order
            columns = ["responseId", "respondentEmail", "createTime"] + [
                resolved_key_for_qid[qid] for qid in qmap.keys()
            ]

            # 4) Build normalized records
            records = []
            for r in records_raw:
                rec = {c: None for c in columns}
                rec["responseId"] = r.get("responseId")
                rec["respondentEmail"] = r.get("respondentEmail")  # requires "collect emails" form setting
                rec["createTime"] = r.get("createTime")

                answers = r.get("answers") or {}
                for qid, ans in answers.items():
                    key = resolved_key_for_qid.get(qid, qid)

                    # Gather text answers (covers MCQ/Checkbox/Dropdown and text/paragraph)
                    vals = []
                    ta = (ans.get("textAnswers") or {}).get("answers") or []
                    vals.extend([a.get("value") for a in ta if isinstance(a, dict) and "value" in a])

                    # Gather file uploads (store fileIds; change to fileName if preferred)
                    fua = (ans.get("fileUploadAnswers") or {}).get("answers") or []
                    vals.extend([a.get("fileId") or a.get("fileName") for a in fua if isinstance(a, dict)])

                    if vals:
                        rec[key] = vals if as_lists else (vals[0] if len(vals) == 1 else ", ".join(vals))

                records.append(rec)

            # Optional: sort chronologically
            records.sort(key=lambda x: (x.get("createTime") or ""))

            return {
                "status": "success",
                "message": f"Retrieved {len(records)} response(s).",
                "response": {"data": json.dumps({"records": records})},
            }

        except Exception as e:
            return {"status": "error", "message": str(e), "response": {}}
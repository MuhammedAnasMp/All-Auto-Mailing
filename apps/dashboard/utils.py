
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account
import os,json




file_path = os.path.join(os.path.dirname(__file__), "service_account.json")

with open(file_path, "r") as f:
    SERVICE_ACCOUNT_INFO = json.load(f)
def upload_to_drive(file_path, folder_id=None):
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    # SERVICE_ACCOUNT_FILE = 'service_account.json'  # your credentials

    credentials = service_account.Credentials.from_service_account_info(
        SERVICE_ACCOUNT_INFO, scopes=SCOPES
    )
    service = build('drive', 'v3', credentials=credentials)

    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id] if folder_id else []
    }

    media = MediaFileUpload(file_path, resumable=True)

    uploaded_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, webViewLink, webContentLink',
        supportsAllDrives=True
    ).execute()

    print(f"✅ Uploaded to Drive: {uploaded_file['webViewLink']}")
    return uploaded_file['webViewLink']
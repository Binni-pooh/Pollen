import pandas as pd
import os
import io
import requests

def load_or_download_csv(url, filename, save_dir="data", delimiter=';', encoding=None):
    """
    Checks if the CSV file exists locally and validates it against the server
    using HTTP conditional requests (If-None-Match / ETag).
    """
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, filename)
    etag_path = file_path + ".etag"
    download_url = url.rstrip('/') + '/' + filename

    # 1. Read stored ETag if it exists locally
    saved_etag = None
    if os.path.exists(etag_path) and os.path.exists(file_path):
        try:
            with open(etag_path, 'r', encoding='utf-8') as f:
                saved_etag = f.read().strip()
        except Exception:
            pass

    # 2. Prepare headers for conditional request
    headers = {}
    if saved_etag:
        headers["If-None-Match"] = saved_etag

    try:
        print(f"Checking server for updates: {download_url}")
        response = requests.get(download_url, headers=headers)

        # Case A: File hasn't changed on the server
        if response.status_code == 304:
            print(f"File is up to date (304 Not Modified). Loading local file from: {file_path}")
            return _read_csv_with_encoding(file_path, delimiter, encoding)

        # Case B: File is new or has been updated on the server
        elif response.status_code == 200:
            print(f"New or updated file found. Downloading...")

            # Use BytesIO to pass downloaded content into your encoding-aware reader
            content_stream = io.BytesIO(response.content)
            df = _read_csv_with_encoding(content_stream, delimiter, encoding)

            # Save the CSV file locally
            df.to_csv(file_path, sep=delimiter, index=False, encoding='utf-8')

            # Save the new ETag provided by the server
            new_etag = response.headers.get("ETag")
            if new_etag:
                with open(etag_path, 'w', encoding='utf-8') as f:
                    f.write(new_etag)

            print(f"File successfully updated and saved to: {file_path}")
            return df

        else:
            print(f"Unexpected HTTP status code ({response.status_code}). Falling back to local file if available.")

    except requests.exceptions.RequestException as e:
        print(f"Network error or connection failed ({e}). Falling back to local cache...")

    # 3. Fallback: If network request failed or returned unexpected status, load local copy if it exists
    if os.path.exists(file_path):
        try:
            print(f"Loading fallback local file from: {file_path}")
            return _read_csv_with_encoding(file_path, delimiter, encoding)
        except Exception as local_e:
            print(f"Error reading local file: {local_e}")
    else:
        print("Error: No local file available and network request failed.")

    return None

def _read_csv_with_encoding(path_or_buffer, delimiter, encoding):
    """Helper function to try multiple encodings if the default fails."""
    if encoding:
        return pd.read_csv(path_or_buffer, delimiter=delimiter, encoding=encoding)

    # Try common encodings for European datasets
    encodings_to_try = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1', 'cp1252']
    for enc in encodings_to_try:
        try:
            # If path_or_buffer is a stream, ensure we reset its pointer if retrying
            if hasattr(path_or_buffer, 'seek'):
                path_or_buffer.seek(0)
            return pd.read_csv(path_or_buffer, delimiter=delimiter, encoding=enc)
        except UnicodeDecodeError:
            continue

    if hasattr(path_or_buffer, 'seek'):
        path_or_buffer.seek(0)
    return pd.read_csv(path_or_buffer, delimiter=delimiter, encoding='utf-8')
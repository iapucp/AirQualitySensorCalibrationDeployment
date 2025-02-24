import re
import os
import io
import boto3
import pandas as pd
from http import HTTPStatus
from datetime import timedelta, datetime
from google.oauth2 import service_account
from botocore.exceptions import ClientError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Path to your credentials JSON file (Downloaded from Google Cloud)
CREDENTIALS_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

# Google Drive Folder ID
FOLDER_ID = os.environ.get("FOLDER_ID", "")

# Authenticate with Google Drive API
SCOPES = ["https://www.googleapis.com/auth/drive"]
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)

# Retry on multiple exceptions: network issues, API errors, IO errors
DOWNLOAD_RETRY_EXCEPTIONS = (HttpError, IOError, ConnectionError, TimeoutError)

# Initialize Google Drive API client
drive_service = build("drive", "v3", credentials=creds)

s3_client = boto3.client("s3")

def get_latest_subfolder_id(parent_id):
    query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder'"
    response = drive_service.files().list(
        q=query,
        fields="files(id, name)"
    ).execute()
    folders = response.get("files", [])
    if not folders:
        print(f"No subfolders found in the folder.")
        return None, None
    # Regex to match folder names like "some_text_MMDDYYYYHHMMSS"
    pattern = re.compile(r"(\d{2})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})$")
    valid_folders = list()
    for folder in folders:
        pattern_match = pattern.search(folder["name"])
        if pattern_match:
            month, day, year, hour, minute, second = map(int, pattern_match.groups())
            folder_date = datetime(year, month, day, hour, minute, second)
            valid_folders.append((folder_date, folder["id"], folder["name"]))
    if not valid_folders:
        print("No subfolders matching the expected date pattern found.")
        return None, None
    # Sort folders by extracted date (newest first)
    latest_folder = max(valid_folders, key=lambda x: x[0])
    return latest_folder[1], latest_folder[2]

def get_latest_excel_file_id_in_folder(folder_id, name_pattern):
    query = (
        f"'{folder_id}' in parents "
        f"and mimeType='text/plain' "
        f"and name contains '{name_pattern}'"
    )
    response = drive_service.files().list(
        q=query,
        fields="files(id, name)",
        orderBy="createdTime desc",  # Order by most recently created file
        # orderBy="modifiedTime desc", # Order by most recently updated file
        pageSize=1  # Get only the latest file
    ).execute()
    files = response.get("files", [])
    if not files:
        print(f"No Excel files matching '{name_pattern}' found in the folder.")
        return None, None
    latest_file = files[0]
    return latest_file["id"], latest_file["name"]

def get_latest_excel_file_id(folder_id, name_pattern):
    subfolder_id, subfolder_name = get_latest_subfolder_id(folder_id)
    if folder_id:
        file_id, file_name = get_latest_excel_file_id_in_folder(subfolder_id, name_pattern)
        return subfolder_id, subfolder_name, file_id, file_name
    return None, None, None, None

# Retry settings: Exponential backoff (1s, 2s, 4s, ...) and stops after 5 attempts
@retry(
    retry=retry_if_exception_type(DOWNLOAD_RETRY_EXCEPTIONS),
    wait=wait_exponential(multiplier=1, min=1, max=10),  # Exponential backoff delay=max(min(multiplier×2^n,max),min)
    stop=stop_after_attempt(5),  # Stop after 5 retries
    retry_error_callback=lambda retry_state: None # Return None after the final retry
)
def download_excel_file(drive_service, file_id):
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request, chunksize=1024*1024) # File will be downloaded in chunks of this many bytes.
    try:
        done = False
        while not done:
            _, done = downloader.next_chunk()
        file_stream.seek(0)
        return file_stream
    except DOWNLOAD_RETRY_EXCEPTIONS as e:
        print(f"Retrying due to error: {e}")
        raise  # Reraises the exception to trigger the retry mechanism
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return None

def read_csv_file(file_stream):
    df = pd.read_csv(file_stream, sep=", ", engine="python")
    return df

def set_date_column_as_index(df, date_column, date_format, date_offset=None):
    df[date_column] = pd.to_datetime(df[date_column], format=date_format)
    if date_offset:
        # Subtract the timedelta (in hours) from the date column to adjust for any time shift
        df[date_column] = df[date_column] - pd.Timedelta(hours=date_offset)
    df.set_index(date_column, inplace=True)
    return df

def get_column_to_remove_nulls(df, columns_processed, n_consecutive_dates):
    df_nulls_per_column = df.drop(columns_processed, axis=1).isna().sum().sort_values(ascending=False)
    df_nulls_per_column = df_nulls_per_column[df_nulls_per_column >= n_consecutive_dates]
    column_with_most_nulls = None
    column_nulls = None
    if len(df_nulls_per_column) > 0:
        column_with_most_nulls = df_nulls_per_column.index[0]
        column_nulls = df_nulls_per_column.iloc[0]
    return column_with_most_nulls, column_nulls

def consecutive_records(dates_dt, timedelta_min, n_consecutive_dates):
    segments = list()
    current_segment = [dates_dt[0]]
    for i in range(1, len(dates_dt)):
        if (dates_dt[i] - dates_dt[i - 1]) == timedelta(minutes=timedelta_min):
            current_segment.append(dates_dt[i])
        else:
            if len(current_segment) >= n_consecutive_dates:
                segments.extend(current_segment)
            current_segment = [dates_dt[i]]
    if len(current_segment) >= n_consecutive_dates:
        segments.extend(current_segment)
    return segments

def remove_nulls(df, column, timedelta_min, n_consecutive_dates):
    null_indices = df.index[df[column].isna()]
    if len(null_indices) >= n_consecutive_dates:
        indices_to_remove = consecutive_records(null_indices, timedelta_min, n_consecutive_dates)
        df = df.drop(indices_to_remove, axis=0)
    return df

def apply_remove_nulls(df, n_consecutive_dates, timedelta_min):
    columns_processed = set()
    print(f"Before removing nulls the shape is {df.shape}.")
    for _ in range(len(df.columns)):
        column_with_most_nulls, column_nulls = get_column_to_remove_nulls(df, columns_processed, n_consecutive_dates)
        if column_with_most_nulls is None:
            return df
        df = remove_nulls(df, column_with_most_nulls, timedelta_min, n_consecutive_dates)
        columns_processed.add(column_with_most_nulls)
        print(f"'{column_with_most_nulls}' had {column_nulls} nulls, 'remove_nulls' was applied.")
        print(f"---> After removing nulls the shape is {df.shape}.")
    return df

def interpolate_nulls(df):
    df_nulls_per_column = df.isna().sum()
    total_nulls = df_nulls_per_column.sum()
    if total_nulls > 0:
        df = df.interpolate(method="linear")
        print(f"There was {total_nulls} nulls in total, 'interpolate_nulls' was applied.")
    return df

def process_dataframe(df, columns_to_keep, standard_date_column, date_format, freq):
    df.columns = df.columns.str.strip()
    df = df[columns_to_keep].copy()
    df.drop_duplicates(subset=[standard_date_column], keep="first", inplace=True)
    df = set_date_column_as_index(df, standard_date_column, date_format) # The date is in GTM-5
    df = df.apply(pd.to_numeric, errors="coerce")
    df.dropna(axis=0, inplace=True)
    # Resample to 1min to standardize the data
    df = df.resample("1min").mean()
    # Remove rows with 61 or more nulls (> 1h)
    df = apply_remove_nulls(df, n_consecutive_dates=61, timedelta_min=1)
    # Impute rows with 60 or less nulls (<= 1h)
    df = interpolate_nulls(df)
    df = df.resample(freq).mean()
    complete_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
    df = df.reindex(complete_index)
    # Subtract 5 minutes from the index
    df.index = df.index - pd.Timedelta(minutes=5)
    return df

def download_file_from_s3(s3_client, local_path, bucket_name, s3_file_name):
    download_path = os.path.join(local_path, os.path.basename(s3_file_name))
    try:
        s3_client.download_file(bucket_name, s3_file_name, download_path)
        return download_path
    except Exception as e:
        return None

# Retry settings: Exponential backoff (1s, 2s, 4s, ...) and stops after 5 attempts
@retry(
    retry=retry_if_exception_type(ClientError),
    wait=wait_exponential(multiplier=1, min=1, max=10),  # Exponential backoff delay=max(min(multiplier×2^n,max),min)
    stop=stop_after_attempt(5),  # Stop after 5 retries
    retry_error_callback=lambda retry_state: False # Assume the file does not exist after the final retry
)
def file_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True  # File exists
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False  # File does not exist
        print(f"Retrying due to error: {e}")
        raise  # Some other error occurred. Reraises the exception to trigger the retry mechanism
    except Exception as e:
        print(f"Error searching the file {key} in the bucket: {e}")
        return False # Assume the file does not exist

def upload_to_s3(s3_client, local_file, bucket_name, s3_file_name):
    # Upload the file
    try:
        s3_client.upload_file(local_file, bucket_name, s3_file_name)
        print(f"File {local_file} successfully uploaded to S3 bucket {bucket_name} as {s3_file_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

def lambda_handler(event, context):
    # Only get Excel files containing "PUCP"
    file_pattern = "PUCP"
    columns_to_keep = ["Date & Time (Local)", "PM2.5 Conc"]
    standard_date_column = "Date & Time (Local)"
    date_format = "%m/%d/%Y %I:%M:%S %p"
    freq = "5min"

    subfolder_id, subfolder_name, file_id, file_name = get_latest_excel_file_id(FOLDER_ID, file_pattern)

    if not file_id:
        print("No matching Excel file found.")
        return

    bucket_name = "air-quality-teledyne"

    local_path = "/tmp"
    s3_file_name = "latest_folder.txt"

    download_path = download_file_from_s3(s3_client, local_path, bucket_name, s3_file_name)

    if download_path:
        with open(download_path, "r") as file_obj:
            latest_subfolder_name = file_obj.read().strip()

        if latest_subfolder_name == subfolder_name:
            print(f"The latest subfolder is still {subfolder_name}")
            return

    file_path = f"{local_path}/{s3_file_name}"

    with open(file_path, "w") as file_obj:
        file_obj.write(subfolder_name + "\n")

    upload_to_s3(s3_client, file_path, bucket_name, s3_file_name)

    print(f"Downloading latest matching file {file_name} from subfolder {subfolder_name}")
    file_stream = download_excel_file(drive_service, file_id)

    if not file_stream:
        print("The excel file was not downloaded")
        return

    print(f"Download finished")
    
    df = read_csv_file(file_stream)

    print(f"Excel file loaded as DataFrame")

    df = process_dataframe(df, columns_to_keep, standard_date_column, date_format, freq)

    print(f"DataFrame processed")

    groups = df.groupby(df.index.normalize())
    num_groups = groups.ngroups

    # Group by only the date (year-month-day)
    for i, (date, group) in enumerate(groups):
        # Use only the year, month, and day
        file_name = f"{date.year}_{date.month:02d}_{date.day:02d}_{freq}_prediction.csv"
        file_path = f"{local_path}/{file_name}"
        group.to_csv(file_path)

        print(f"Teledyne data saved to {file_path}")

        # The filename on S3 will be the same as the local filename
        s3_file_name = file_name

        is_last = (i == num_groups - 1)

        if not is_last:
            s3_file_name_exists = file_exists(s3_client, bucket_name, s3_file_name)

            if not s3_file_name_exists:
                # Upload the file to S3
                upload_to_s3(s3_client, file_path, bucket_name, s3_file_name)
            else:
                print(f"File {s3_file_name} already exists, skipping upload.")
        else:
            # Upload the file to S3
            upload_to_s3(s3_client, file_path, bucket_name, s3_file_name)

        # Remove the local file after upload
        os.remove(file_path)
# lambda_handler({}, None)
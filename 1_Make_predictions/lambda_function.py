from http import HTTPStatus
from datetime import datetime, timedelta
from models_configuration import model_paths
import os
import time
import json
import pytz
import boto3
import joblib
import requests
import pandas as pd

def convert_gmt5_to_utc(gmt5_date):
    utc_date = gmt5_date.astimezone(pytz.utc)
    return utc_date

def get_start_end_of_day_in_utc():
    # Define the GMT-5 timezone using pytz
    gmt5 = pytz.timezone("Etc/GMT+5")

    # Get the current time in GMT-5
    gmt5_now = datetime.now(gmt5)

    # Start of the current day in GMT-5 (00:00:00)
    start_of_current_day_gmt5 = gmt5_now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # The start date is 24 hours before the start of the current day
    start_of_day_gmt5 = start_of_current_day_gmt5 - timedelta(days=1)

    # The end date is the start of the current day minus 1 second
    end_of_day_gmt5 = start_of_current_day_gmt5 - timedelta(days=0) - timedelta(seconds=1)
    
    # Convert both start and end times to UTC
    start_of_day_utc = convert_gmt5_to_utc(start_of_day_gmt5)
    end_of_day_utc = convert_gmt5_to_utc(end_of_day_gmt5)
    
    return start_of_day_utc, end_of_day_utc

def format_datetime(date_obj):
    return date_obj.strftime("%d-%m-%Y %H:%M:%S")

def extract_token_from_response(json_response):
    try:
        token = json_response["jwt"]
        return token
    except json.JSONDecodeError:
        print("Failed to decode the response as JSON. The response might not be in JSON format.")
        return None
    except KeyError:
        print("The 'jwt' key was not found in the JSON response.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return None

def request_with_retries(method, url, headers, data, max_retries=3, delay=2):
    for attempt in range(1, max_retries + 1):
        try:
            if method == "POST":
                response = requests.post(url, headers=headers, data=data)
            elif method == "GET":
                response = requests.get(url, headers=headers, data=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check if the response status code is 2xx (successful)
            if 200 <= response.status_code < 300:
                # Return the JSON response if successful
                return response.json()
            
            # If the status code is not 200, raise an exception to retry
            response.raise_for_status()
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"Attempt {attempt} failed: {e}")
            
            # If we've reached the max retries, return None
            if attempt == max_retries:
                print("Max retries reached. Returning None.")
                return None
            
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)

def get_login_token(url, headers, data, max_retries=3, delay=2):
    response_data = request_with_retries("POST", url, headers=headers, data=data, max_retries=max_retries, delay=delay)
    if response_data:
        return extract_token_from_response(response_data)
    return None

def get_data_with_retries(url, headers, data, max_retries=3, delay=2):
    response_data = request_with_retries("GET", url, headers=headers, data=data, max_retries=max_retries, delay=delay)
    if response_data:
        return response_data["data"]
    return None

def process_data(records, column_mapping, model_columns, date_column):
    df = pd.DataFrame(records)

    # Rename columns
    df.rename(columns=column_mapping, inplace=True)

    # Convert the date column to datetime for proper sorting
    df[date_column] = pd.to_datetime(df[date_column], format="%a, %d %b %Y %H:%M:%S GMT")

    # Sort by the date column in ascending order
    df.sort_values(by=date_column, ascending=True, inplace=True)

    # Subtract the timedelta (in hours) from the date column to adjust to GMT-5
    df[date_column] = df[date_column] - pd.Timedelta(hours=5)

    # Set the date column as the index
    df.set_index(date_column, inplace=True)

    # Drop rows with missing values in the model_columns
    df.dropna(subset=model_columns, inplace=True)

    return df

def load_model(qhawax_id):
    # Check if the qhawax_id exists in the configuration
    if qhawax_id not in model_paths:
        raise ValueError(f"qhawax_id {qhawax_id} not found in the models configuration file.")

    # Get the model file name from the configuration
    model_file = model_paths[qhawax_id]

    # Get the current working directory as the base path
    base_path = os.getcwd()

    # Construct the full path to the model file
    artifact_path = os.path.join(base_path, model_file)

    # Check if the file exists
    if not os.path.isfile(artifact_path):
        raise FileNotFoundError(f"Model file {model_file} for qhawax_id {qhawax_id} not found at {artifact_path}")
    
    # Load the model from the .pkl file
    with open(artifact_path, "rb") as grid_search_file:
        grid_search = joblib.load(grid_search_file)

    # Return the best estimator (assuming grid search model)
    model = grid_search.best_estimator_
    return model

def upload_to_s3(local_file, bucket_name, s3_file_name):
    s3 = boto3.client("s3")

    # Upload the file
    try:
        s3.upload_file(local_file, bucket_name, s3_file_name)
        print(f"File {local_file} successfully uploaded to S3 bucket {bucket_name} as {s3_file_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

def lambda_handler(event, context):
    start_date, end_date = get_start_end_of_day_in_utc()
    start_date_formatted, end_date_formatted = format_datetime(start_date), format_datetime(end_date)

    print("Start of the day in UTC:", start_date_formatted)
    print("End of the day in UTC:", end_date_formatted)

    login_url = "https://qhapi.qairadrones.com/api/v2/login/"

    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")

    headers = {
        "Content-Type": "application/json"
    }
    data = json.dumps({
        "email": email,
        "password": password
    })

    token = get_login_token(login_url, headers, data)

    if token is None:
        print("Failed to get login token, aborting.")
        return
        #exit()

    # print("Login Token:", token)

    url = f"https://qhapi.qairadrones.com/api/external/get_promedio_5min_qhawax_all/?initial_timestamp={start_date_formatted}&final_timestamp={end_date_formatted}&qhawax_ids=13,14,15,17,18"

    headers = {
        "Authorization": f"Bearer {token}"
    }
    data = {}

    response_data = get_data_with_retries(url, headers, data)
    if response_data is None:
        print("Failed to get sensors data, aborting.")
        return
        #exit()

    ################################################################################################################################
    # with open("response_data.json", 'w') as json_file:
    #     json.dump(response_data, json_file, indent=0)
    ################################################################################################################################
    # with open("response_data.json", "r") as json_file:
    #     response_data = json.load(json_file)
    ################################################################################################################################

    dfs = list()

    qhawax_ids_with_models = {"qH013", "qH014", "qH015", "qH017", "qH018"}
    column_mapping = {
        "PM25": "Pm2.5",
        "temperature": "Temp",
        "humidity": "Humedad",
        "pressure": "Presion"
    }
    model_columns = ["Pm2.5", "Temp", "Humedad", "Presion"]
    date_column = "timestamp_zone"
    freq = "5min"

    # Iterate through each qHAWAX key in the original dictionary
    for qhawax_id, data_and_metadata in response_data.items():
        if qhawax_id not in qhawax_ids_with_models: continue

        print(f"Processing qHAWAX {qhawax_id}")

        records = data_and_metadata[0].get("data", None)
        
        if not records: continue

        # Process the records using the specified mappings and columns
        df = process_data(records, column_mapping, model_columns, date_column)

        # Load the pre-trained model for the current qhawax_id
        model = load_model(qhawax_id)

        # Extract the features for prediction (using the model_columns)
        x = df[model_columns]
        
        # Predict the target values (Pm2.5) using the loaded model
        y_predicted = model.predict(x)

        # Assign the predicted values to the 'Prediccion_Pm2.5' column in the dataframe
        df["Prediccion_Pm2.5"] = y_predicted
        
        # Remove duplicated indexes
        df = df[~df.index.duplicated(keep="first")]

        # Create a complete date range for the index with a frequency of 5 min
        complete_index = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        
        # Reindex the dataframe to the complete date range to fill in missing time slots
        df = df.reindex(complete_index)

        # Set the 'qhawax_id' to avoid NaN values for this column
        df["qhawax_id"] = qhawax_id

        # Append the processed dataframe for the current qhawax_id to the list of dataframes
        dfs.append(df)

    # Concatenate all dataframes
    final_df = pd.concat(dfs, axis=0, ignore_index=False)

    final_df = final_df.drop("id", axis=1)

    # Use only the year, month, and day from start_date
    file_name = f"{start_date.year}_{start_date.month:02d}_{start_date.day:02d}_{freq}_prediction.csv"
    file_path = f"/tmp/{file_name}"
    final_df.to_csv(file_path)

    print(f"Data with predictions saved to {file_path}")

    bucket_name = "air-quality-predictions"

    # The filename on S3 will be the same as the local filename
    s3_file_name = file_name

    # Upload the file to S3
    upload_to_s3(file_path, bucket_name, s3_file_name)

    # Remove the local file after upload
    os.remove(file_path)
# lambda_handler({}, None)
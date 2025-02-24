import pandas as pd
import boto3
import json
import jwt
import os
import io

from datetime import datetime, timedelta
from http import HTTPStatus

# Secret key for decoding the JWT
SECRET_KEY = os.getenv("JWT_SECRET")

# Initialize S3 client
s3_client = boto3.client("s3")

def verify_token(token):
    """Verifies the JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload  # Returns the decoded payload if valid
    except jwt.ExpiredSignatureError:
        return {"error": "Token has expired"}
    except jwt.InvalidTokenError:
        return {"error": "Invalid token"}

def get_data_as_dataframe(s3_client, bucket_name, start_filename, start_date, end_date, columns_to_keep):
    # List all objects in the S3 bucket, starting after the start_filename
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        StartAfter=start_filename  # Start listing after this filename
    )
    
    # Prepare a list to store results
    results = list()

    # Loop over the files in the bucket
    for obj in response.get("Contents", list()):
        file_name = obj["Key"]

        if not file_name.endswith(".csv"): continue
        
        # Extract the date from the file name (assuming the format is 'YYYY_MM_DD_5min_prediction.csv')
        try:
            file_date_str = file_name.split("_")[0:3]
            file_date = datetime.strptime("-".join(file_date_str), "%Y-%m-%d")
        except Exception as e:
            continue
        
        # Check if the file is within the specified date range
        if start_date <= file_date <= end_date:
            # Read the CSV file from S3 into a pandas dataframe
            csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            file_content = csv_file["Body"].read()
            
            # Read CSV into dataframe, parse the first column as datetime, and set it as the index
            df = pd.read_csv(io.BytesIO(file_content), sep=",", index_col=0, parse_dates=True)
            
            # Extract the relevant columns
            df_filtered = df[columns_to_keep].copy()
            
            # Use the index (which is datetime) as the 'date' column
            df_filtered["date"] = df.index.strftime("%d-%m-%Y %H:%M:%S")
            
            # Append to the results list
            results.append(df_filtered)

    final_results = None
    if len(results) > 0:
        # Concatenate all dataframes into one
        final_results = pd.concat(results, axis=0, ignore_index=True)
        final_results.drop_duplicates(keep="first", inplace=True)

    return final_results

def lambda_handler(event, context):
    try:
        headers = event.get("headers", {})
        auth_header = headers.get("authorization")
        if not auth_header:
            auth_header = headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": json.dumps({"message": "Missing or invalid Authorization header"})
            }

        token = auth_header.split(" ")[1]
        decoded_payload = verify_token(token)

        if "error" in decoded_payload:
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": json.dumps(decoded_payload)
            }

        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": json.dumps({"message": "Invalid JSON format in request body"})
            }

        # Get the start and end date
        start_date = body.get("start_date", None)
        end_date = body.get("end_date", None)

        if (start_date is None) or (end_date is None):
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": {"message": "The parameters start_date and end_date cannot be None"}
            }

        freq = "5min"
        
        # Convert the dates from string to datetime objects for comparison
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": {"message": "Invalid date format. Please use YYYY-MM-DD."}
            }

        # Validate that end_date is greater than or equal to start_date
        if end_date < start_date:
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": {"message": "The end_date must be greater than or equal to start_date."}
            }

        # Subtract 1 hour from the start_date for a correct search with StartAfter
        adjusted_start_date = start_date - timedelta(hours=1)
        
        # Define the start filename based on the start_date
        start_filename = adjusted_start_date.strftime("%Y_%m_%d") + f"_{freq}_prediction.csv"

        bucket_name = "air-quality-predictions"
        columns_to_keep = ["PM10", "Pm2.5", "Prediccion_Pm2.5", "qhawax_id"]

        df_predictions = get_data_as_dataframe(s3_client, bucket_name, start_filename, start_date, end_date, columns_to_keep)

        if not isinstance(df_predictions, pd.DataFrame):
            return {
                "statusCode": HTTPStatus.OK,
                "body": json.dumps([])
            }

        bucket_name = "air-quality-teledyne"
        columns_to_keep = ["PM2.5 Conc"]

        df_teledyne = get_data_as_dataframe(s3_client, bucket_name, start_filename, start_date, end_date, columns_to_keep)

        if isinstance(df_teledyne, pd.DataFrame):
            df_teledyne.set_index("date", inplace=True)
            df_predictions.set_index("date", inplace=True)

            df_predictions["Teledyne_pm2.5"] = df_teledyne["PM2.5 Conc"].reindex(df_predictions.index)

            df_predictions.reset_index(drop=False, inplace=True)

        # Convert the DataFrame to a JSON serializable format (list of dictionaries)
        df_predictions_serializable = df_predictions.to_dict(orient="records")

        ################################################################################################################################
        # with open("response_data_real_time.json", 'w') as json_file:
        #     json.dump(df_predictions_serializable, json_file, indent=0)
        ################################################################################################################################
        
        # Return the results as a JSON response
        return {
            "statusCode": HTTPStatus.OK,
            "body": json.dumps(df_predictions_serializable)
        }
    except Exception as e:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "body": json.dumps({"error": str(e)})
        }
# print(lambda_handler({"body": json.dumps({"start_date": "2025-03-01", "end_date": "2025-03-28"})}, None))
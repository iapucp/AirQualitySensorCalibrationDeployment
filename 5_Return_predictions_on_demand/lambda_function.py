import pandas as pd
import joblib
import boto3
import json
import jwt
import os
import io

from models_configuration import model_paths
from datetime import datetime, timedelta
from http import HTTPStatus

# Secret key for decoding the JWT
SECRET_KEY = os.getenv("JWT_SECRET")

# Initialize S3 client
s3_client = boto3.client("s3")

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

def verify_token(token):
    """Verifies the JWT token."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload  # Returns the decoded payload if valid
    except jwt.ExpiredSignatureError:
        return {"error": "Token has expired"}
    except jwt.InvalidTokenError:
        return {"error": "Invalid token"}

def make_prediction(data, model):
    """Makes predictions using the model on the provided data."""
    # Convert data to pandas DataFrame for easy processing
    df = pd.DataFrame(data)

    # Ensure that the features are available for prediction
    model_columns = ["PM25", "temperature", "humidity", "pressure"]
    if not all(col in df.columns for col in model_columns):
        raise ValueError("The input data is missing one or more required features.")

    # Extract features
    X = df[model_columns]

    column_mapping = {
        "PM25": "Pm2.5",
        "temperature": "Temp",
        "humidity": "Humedad",
        "pressure": "Presion"
    }

    # Rename columns
    X.rename(columns=column_mapping, inplace=True)

    # Make predictions
    predictions = model.predict(X)

    # Return the results as a list of values
    return predictions.tolist()

def lambda_handler(event, context):
    try:
        # Verify the Token
        headers = event.get("headers", {})
        auth_header = headers.get("authorization")
        if not auth_header:
            auth_header = headers.get("Authorization")

        if not auth_header or not auth_header.startswith("Bearer "):
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": json.dumps({"message": "Missing or invalid Authorization header"})
            }

        token_prefix = "Bearer "
        token = auth_header[len(token_prefix):] if auth_header.startswith(token_prefix) else None

        if not token:
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": json.dumps({"message": "Missing token in Authorization header"})
            }

        decoded_payload = verify_token(token)

        if "error" in decoded_payload:
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": json.dumps(decoded_payload)
            }

        # Parse input data for prediction
        try:
            body = json.loads(event.get("body", "{}"))
        except json.JSONDecodeError:
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": json.dumps({"message": "Invalid JSON format in request body"})
            }

        # Ensure required fields are present
        if "qhawax_id" not in body or "data" not in body:
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": json.dumps({"message": "Missing required fields: qhawax_id and data"})
            }

        qhawax_id = body["qhawax_id"]
        data = body["data"]

        # Ensure data is a list of dictionaries
        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            return {
                "statusCode": HTTPStatus.BAD_REQUEST,
                "body": json.dumps({"message": "Data must be a list of dictionaries"})
            }

        # Load the model based on qhawax_id
        model = load_model(qhawax_id)

        # Make batch predictions
        prediction_results = make_prediction(data, model)

        # Return prediction results in response
        return {
            "statusCode": HTTPStatus.OK,
            "body": json.dumps({"predictions": prediction_results})
        }
    except ValueError as e:
        return {
            "statusCode": HTTPStatus.BAD_REQUEST,
            "body": json.dumps({"message": str(e)})
        }
    except FileNotFoundError as e:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "body": json.dumps({"message": str(e)})
        }
    except Exception as e:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "body": json.dumps({"error": str(e)})
        }
# print(
#     lambda_handler(
#         {
#             "headers": {
#                 "Authorization": "Bearer <your-jwt-token-here>"
#             },
#             "body": json.dumps(
#                 {
#                     "qhawax_id": "qH013",
#                     "data": [
#                         {"PM25": 12.5, "temperature": 22, "humidity": 75, "pressure": 1013},
#                         {"PM25": 13.2, "temperature": 21.8, "humidity": 78, "pressure": 1011}
#                     ]
#                 }
#             )
#         },
#         None
#     )
# )

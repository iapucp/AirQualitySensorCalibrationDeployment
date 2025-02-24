from http import HTTPStatus
import datetime
import json
import jwt
import os

EMAIL = os.getenv("EMAIL")
PASSWORD = os.getenv("PASSWORD")

# Secret key for encoding and decoding the JWT
SECRET_KEY = os.getenv("JWT_SECRET")

def generate_token(email):
    """Generates a JWT token valid for 1 min."""
    payload = {
        "email": email,
        "exp": (datetime.datetime.utcnow() + datetime.timedelta(minutes=1)).timestamp()
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def lambda_handler(event, context):
    """Handles user authentication and returns a JWT token."""
    try:
        body = json.loads(event.get("body", "{}"))

        email = body.get("email")
        password = body.get("password")

        if email == EMAIL and password == PASSWORD:
            token = generate_token(email)
            return {
                "statusCode": HTTPStatus.OK,
                "body": json.dumps({"token": token})
            }
        else:
            return {
                "statusCode": HTTPStatus.UNAUTHORIZED,
                "body": {"message": "Invalid credentials"}
            }
    except Exception as e:
        return {
            "statusCode": HTTPStatus.INTERNAL_SERVER_ERROR,
            "body": json.dumps({"error": str(e)})
        }

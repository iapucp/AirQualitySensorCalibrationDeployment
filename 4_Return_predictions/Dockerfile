FROM python:3.10-slim

# Set the working directory in the Docker image
WORKDIR /app

RUN mkdir -p /app

COPY ./requirements.txt /app

# Install the python requirements from requirements.txt
RUN python3.10 -m pip install -r ./requirements.txt

COPY ./*.py /app/

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Pass the name of the function handler as an argument to the runtime
CMD [ "lambda_function.lambda_handler" ]

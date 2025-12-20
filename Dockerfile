# Use an official Python 3.11 runtime as a parent image
# We recommend using a specific version of Python for stability and reproducibility
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
# This is done early to leverage Docker's caching for dependencies
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# # Copy the rest of the application code into the container at /app
COPY . .

# # Expose the port that your FastAPI application listens on
# # Default for FastAPI is usually 8000
EXPOSE 8002

# # Define environment variables (optional, but good practice for Uvicorn)
ENV PYTHONPATH=/app
ENV UVICORN_PORT=8002
ENV UVICORN_HOST=0.0.0.0

# # Command to run the application using Uvicorn
# # You'll need to replace 'app.main:app' with the correct path to your FastAPI instance.
# # For example, if your app is in 'app/main.py' and your FastAPI instance is called 'app',
# # it would be `uvicorn app.main:app`
# # CMD ["/usr/local/bin/python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8002"]
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "--bind", "0.0.0.0:8002", "-t", "180"]
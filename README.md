# insurance-quotes
Provides insurance quotes for a given insurance policy information for a given insurance carriers

## Development Setup

1.  **Create a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Running the Service

To run the application locally you can launch main.py in debug mode pointing to local .env file.


To run the application locally for development, use `uvicorn` with auto-reloading:

```bash
uvicorn src.insurance_quotes.main:app --reload
```

The API will be available at `http://127.0.0.1:8002`. You can access the interactive API documentation (Swagger UI) at `http://127.0.0.1:8002/docs`.

### API Endpoints
*   **Health Check:** `GET /health` - Returns `{"status": "ok"}` if the service is running.


3. ## Docker
a. ### Building

cd project_root
docker build -t insurance-quotes .

aws ecr get-login-password --region us-west-1 | docker login --username AWS --password-stdin  889572107296.dkr.ecr.us-west-1.amazonaws.com

docker buildx build --no-cache --platform linux/amd64 -t insurance-quotes .
docker tag insurance-quotes:latest 889572107296.dkr.ecr.us-west-1.amazonaws.com/insurance-quotes-app:latest
docker push 889572107296.dkr.ecr.us-west-1.amazonaws.com/insurance-quotes-app:latest

b. ### Launching
docker run -p 8002:8002 --network coveragecompassnetwork  --env-file .env insurance-quotes

Bashing into 
docker run -it --name insurance-quotes bash 



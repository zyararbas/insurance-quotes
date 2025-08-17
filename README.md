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

To run the application locally for development, use `uvicorn` with auto-reloading:

```bash
uvicorn src.insurance_quotes.main:app --reload
```

The API will be available at `http://127.0.0.1:8000`. You can access the interactive API documentation (Swagger UI) at `http://127.0.0.1:8000/docs`.

### API Endpoints
*   **Health Check:** `GET /health` - Returns `{"status": "ok"}` if the service is running.

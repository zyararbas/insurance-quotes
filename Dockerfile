# Python 3.11 runtime, pinned for reproducibility
FROM python:3.11-slim

WORKDIR /app

# Install dependencies first so this layer is cached across code changes.
# torch+cpu is resolved from the PyTorch index.
COPY requirements.txt .
RUN pip install --no-cache-dir --extra-index-url https://download.pytorch.org/whl/cpu -r requirements.txt

# --- Pre-bake the embedding model -------------------------------------------
# The Chroma collection is queried with SentenceTransformer "all-mpnet-base-v2".
# Downloading it (~420MB) from HuggingFace on every cold start is slow, so we
# fetch it at build time into an in-image cache. HF_HOME must stay set at runtime
# so the app loads from this cache instead of re-downloading.
ENV HF_HOME=/opt/hf
ENV SENTENCE_TRANSFORMERS_HOME=/opt/hf
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-mpnet-base-v2')"

# Force offline mode at RUNTIME (set AFTER the download above). Without this,
# sentence-transformers/huggingface_hub still makes an online HEAD request per
# model file to revalidate the cache on startup — which, if the network is slow
# or blocked, retries 5x per file and makes cold start far worse. Offline mode
# loads straight from the baked cache with zero network calls.
ENV HF_HUB_OFFLINE=1
ENV TRANSFORMERS_OFFLINE=1

# --- Pre-bake the vehicle-rates Chroma DB -----------------------------------
# initialize_vehicle_rates_chromadb() uses ./vehicle_rates_chroma_db if present
# and only falls back to an S3 download when it is missing. Copying it here (its
# own ~281MB layer, changes rarely) makes the image self-contained: no S3 call,
# no boto3 credentials, near-instant startup. Fail the build loudly if the dir
# was not in the build context (it is gitignored, so it must exist on disk).
COPY vehicle_rates_chroma_db/ ./vehicle_rates_chroma_db/
RUN test -f ./vehicle_rates_chroma_db/chroma.sqlite3 \
    || (echo "FATAL: vehicle_rates_chroma_db missing from build context — cannot bake image." && exit 1)

# Application code (last, so edits don't bust the heavy layers above)
COPY . .

EXPOSE 8002

ENV PYTHONPATH=/app
ENV UVICORN_PORT=8002
ENV UVICORN_HOST=0.0.0.0

# --forwarded-allow-ips="*" so gunicorn trusts Cloud Run's X-Forwarded-Proto and
# emits https (not http) redirects (otherwise POST trailing-slash redirects 405).
CMD ["gunicorn", "-w", "4", "-k", "uvicorn.workers.UvicornWorker", "app.main:app", "--bind", "0.0.0.0:8002", "-t", "180", "--forwarded-allow-ips=*"]

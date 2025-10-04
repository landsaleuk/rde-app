FROM python:3.11-slim

# (Only if you need GDAL for ogr2ogr inside this image — you do for your earlier imports)
RUN apt-get update && apt-get install -y --no-install-recommends \
      gdal-bin libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first so Docker can cache pip layers intelligently
COPY requirements.txt /app/requirements.txt

# Always upgrade pip tooling (prevents wheel issues), then install deps
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the application
COPY . /app

# Start FastAPI
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]
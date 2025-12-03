FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir \
    streamlit>=1.28.0 \
    polars>=0.20.0 \
    plotly>=5.18.0 \
    google-cloud-storage>=2.14.0 \
    pyarrow>=14.0.0

# Copy application code
COPY . .

# Expose port for Cloud Run
EXPOSE 8080

# Health check
HEALTHCHECK CMD curl --fail http://localhost:8080/_stcore/health || exit 1

# Run Streamlit
ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]


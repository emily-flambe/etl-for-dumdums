# Streamlit app deployment for Google Cloud Run
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
RUN pip install uv

# Copy dependency files first (for layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies using uv
RUN uv sync --frozen --no-dev

# Copy application code
COPY app.py Summary.py data.py ./
COPY pages/ ./pages/
COPY lib/ ./lib/
COPY sources/ ./sources/

# Set environment variables
ENV DEPLOYMENT_MODE=public
ENV STREAMLIT_SERVER_PORT=8080
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

# Cloud Run sets PORT env var - Streamlit needs to use it
# The entrypoint script will handle this

# Expose port (Cloud Run uses 8080 by default)
EXPOSE 8080

# Run Streamlit
CMD ["uv", "run", "streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0"]

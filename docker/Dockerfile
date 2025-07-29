# Multi-stage Docker build for trading platform

# Stage 1: Base Python environment
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set work directory
WORKDIR /app

# Stage 2: Dependencies
FROM base as dependencies

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Stage 3: Application
FROM dependencies as application

# Copy application code
COPY . .

# Change ownership to app user
RUN chown -R appuser:appuser /app

# Switch to app user
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["python", "-m", "trading_platform.main"]

# Stage 4: Development environment
FROM application as development

# Switch back to root for development tools
USER root

# Install development dependencies
RUN pip install \
    jupyter \
    pytest \
    pytest-cov \
    black \
    flake8 \
    mypy \
    pre-commit

# Install Jupyter extensions
RUN jupyter contrib nbextension install --system && \
    jupyter nbextension enable --py widgetsnbextension

# Create Jupyter config
RUN mkdir -p /home/appuser/.jupyter && \
    echo "c.NotebookApp.token = ''" > /home/appuser/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.password = ''" >> /home/appuser/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.open_browser = False" >> /home/appuser/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.ip = '0.0.0.0'" >> /home/appuser/.jupyter/jupyter_notebook_config.py && \
    echo "c.NotebookApp.port = 8888" >> /home/appuser/.jupyter/jupyter_notebook_config.py

# Change ownership back to app user
RUN chown -R appuser:appuser /home/appuser/.jupyter

# Switch back to app user
USER appuser

# Expose Jupyter port
EXPOSE 8888

# Stage 5: Testing environment
FROM dependencies as testing

# Copy application code
COPY . .

# Install test dependencies
RUN pip install \
    pytest \
    pytest-cov \
    pytest-mock \
    coverage

# Change ownership to app user
RUN chown -R appuser:appuser /app

# Switch to app user
USER appuser

# Run tests by default
CMD ["python", "-m", "pytest", "tests/", "-v", "--cov=trading_platform"]

# Stage 6: Production environment
FROM application as production

# Remove development files
RUN find . -name "*.pyc" -delete && \
    find . -name "__pycache__" -type d -exec rm -rf {} + && \
    rm -rf tests/ notebooks/ docs/

# Production command
CMD ["python", "-m", "trading_platform.main", "--env=production"]

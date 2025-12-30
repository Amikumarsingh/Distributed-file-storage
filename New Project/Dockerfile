FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Generate gRPC code
RUN python -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/*.proto

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 8001

# Set environment variables
ENV PYTHONPATH=/app
ENV DATA_DIR=/data

# Default command
CMD ["python", "-m", "dfs.node.server", "--address", "0.0.0.0", "--port", "8001", "--data-dir", "/data"]
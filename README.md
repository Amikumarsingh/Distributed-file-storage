# Distributed File Storage System

A production-ready peer-to-peer distributed file storage system built with Python and gRPC.

## Features

- **Distributed Architecture**: Nodes communicate via gRPC
- **Consistent Hashing**: Efficient file distribution across nodes
- **Replication**: Configurable replication factor for fault tolerance
- **Data Integrity**: SHA-256 checksums for corruption detection
- **Auto-Recovery**: Automatic replica restoration on node failures
- **Versioning**: File version management
- **Monitoring**: Comprehensive logging and health checks
- **Modern UI**: CLI and Web interface options

## Quick Start

### Using Docker Compose

```bash
# Start a 3-node cluster
docker-compose up -d

# Upload a file
python client/cli.py upload test.txt

# Download a file
python client/cli.py download test.txt

# List files
python client/cli.py list
```

### Manual Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Generate gRPC code
python -m grpc_tools.protoc --proto_path=proto --python_out=. --grpc_python_out=. proto/*.proto

# Start nodes
python -m dfs.node.server --port 8001 --data-dir ./data1
python -m dfs.node.server --port 8002 --data-dir ./data2 --peers localhost:8001
python -m dfs.node.server --port 8003 --data-dir ./data3 --peers localhost:8001
```

## Architecture

The system consists of:

- **Node Server**: Core storage node with gRPC services
- **Consistent Hash Ring**: Determines file placement
- **Replication Manager**: Handles data replication
- **Storage Engine**: File I/O operations
- **Client API**: gRPC client interface
- **CLI/Web UI**: User interfaces

## Configuration

Key settings in `config.yaml`:

```yaml
replication_factor: 3
hash_ring_size: 1024
storage_path: "./data"
log_level: "INFO"
```

## API

### gRPC Services

- `UploadFile`: Store files in the cluster
- `DownloadFile`: Retrieve files from the cluster
- `DeleteFile`: Remove files from the cluster
- `ListFiles`: List all stored files
- `HealthCheck`: Node health status
- `JoinCluster`: Add node to cluster
- `LeaveCluster`: Remove node from cluster

### CLI Commands

```bash
# File operations
python client/cli.py upload <file_path>
python client/cli.py download <file_name> [output_path]
python client/cli.py delete <file_name>
python client/cli.py list

# Cluster operations
python client/cli.py status
python client/cli.py rebalance
```

## Development

### Running Tests

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# All tests
pytest
```

### Code Quality

```bash
# Linting
flake8 dfs/
black dfs/
mypy dfs/
```

## Deployment

### Docker

```bash
# Build images
docker build -t dfs-node .

# Run cluster
docker-compose up -d
```

### Kubernetes

```bash
kubectl apply -f k8s/
```

## Monitoring

- Logs: Structured JSON logging
- Metrics: Node health, storage usage, request latency
- Health checks: Built-in gRPC health service

## License

MIT License


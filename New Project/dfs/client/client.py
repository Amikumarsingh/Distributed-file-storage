"""Distributed file storage client."""

import grpc
from typing import List, Optional, Dict, Any
from pathlib import Path

from proto import storage_pb2_grpc, storage_pb2
from dfs.utils.crypto import calculate_checksum
from dfs.utils.logging import get_logger


class DFSClient:
    """Client for interacting with the distributed file storage system."""
    
    def __init__(self, nodes: List[str]):
        """Initialize client with list of node addresses."""
        self.nodes = nodes
        self.logger = get_logger("client")
    
    async def _get_healthy_node(self) -> Optional[str]:
        """Get a healthy node from the cluster."""
        for node in self.nodes:
            try:
                channel = grpc.aio.insecure_channel(node)
                stub = storage_pb2_grpc.ClusterManagerStub(channel)
                
                request = storage_pb2.HealthCheckRequest()
                response = await stub.HealthCheck(request)
                await channel.close()
                
                if response.healthy:
                    return node
                    
            except Exception:
                continue
        
        return None
    
    async def upload_file(self, file_path: str, metadata: Dict[str, str] = None) -> bool:
        """Upload a file to the distributed storage."""
        try:
            # Read file data
            path = Path(file_path)
            if not path.exists():
                self.logger.error(f"File not found: {file_path}")
                return False
            
            with open(path, 'rb') as f:
                data = f.read()
            
            # Calculate checksum
            checksum = calculate_checksum(data)
            
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return False
            
            # Upload file
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.FileStorageStub(channel)
            
            request = storage_pb2.UploadFileRequest(
                filename=path.name,
                data=data,
                checksum=checksum,
                metadata=metadata or {}
            )
            
            response = await stub.UploadFile(request)
            await channel.close()
            
            if response.success:
                self.logger.info(f"Successfully uploaded {path.name}")
                return True
            else:
                self.logger.error(f"Upload failed: {response.message}")
                return False
                
        except Exception as e:
            self.logger.error(f"Upload error: {e}")
            return False
    
    async def download_file(self, filename: str, output_path: str = None) -> bool:
        """Download a file from the distributed storage."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return False
            
            # Download file
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.FileStorageStub(channel)
            
            request = storage_pb2.DownloadFileRequest(filename=filename)
            response = await stub.DownloadFile(request)
            await channel.close()
            
            if not response.success:
                self.logger.error(f"Download failed: {response.message}")
                return False
            
            # Verify checksum
            actual_checksum = calculate_checksum(response.data)
            if actual_checksum != response.checksum:
                self.logger.error("Data corruption detected during download")
                return False
            
            # Write file
            output_file = output_path or filename
            with open(output_file, 'wb') as f:
                f.write(response.data)
            
            self.logger.info(f"Successfully downloaded {filename}")
            return True
            
        except Exception as e:
            self.logger.error(f"Download error: {e}")
            return False
    
    async def delete_file(self, filename: str) -> bool:
        """Delete a file from the distributed storage."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return False
            
            # Delete file
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.FileStorageStub(channel)
            
            request = storage_pb2.DeleteFileRequest(filename=filename)
            response = await stub.DeleteFile(request)
            await channel.close()
            
            if response.success:
                self.logger.info(f"Successfully deleted {filename}")
                return True
            else:
                self.logger.error(f"Delete failed: {response.message}")
                return False
                
        except Exception as e:
            self.logger.error(f"Delete error: {e}")
            return False
    
    async def list_files(self, prefix: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        """List files in the distributed storage."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return []
            
            # List files
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.FileStorageStub(channel)
            
            request = storage_pb2.ListFilesRequest(prefix=prefix, limit=limit)
            response = await stub.ListFiles(request)
            await channel.close()
            
            files = []
            for file_info in response.files:
                files.append({
                    "filename": file_info.filename,
                    "size": file_info.size,
                    "checksum": file_info.checksum,
                    "created_at": file_info.created_at,
                    "modified_at": file_info.modified_at,
                    "version": file_info.version,
                    "replicas": list(file_info.replicas),
                    "metadata": dict(file_info.metadata)
                })
            
            return files
            
        except Exception as e:
            self.logger.error(f"List files error: {e}")
            return []
    
    async def get_file_info(self, filename: str) -> Optional[Dict[str, Any]]:
        """Get information about a specific file."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return None
            
            # Get file info
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.FileStorageStub(channel)
            
            request = storage_pb2.GetFileInfoRequest(filename=filename)
            response = await stub.GetFileInfo(request)
            await channel.close()
            
            if not response.success:
                return None
            
            file_info = response.file_info
            return {
                "filename": file_info.filename,
                "size": file_info.size,
                "checksum": file_info.checksum,
                "created_at": file_info.created_at,
                "modified_at": file_info.modified_at,
                "version": file_info.version,
                "replicas": list(file_info.replicas),
                "metadata": dict(file_info.metadata)
            }
            
        except Exception as e:
            self.logger.error(f"Get file info error: {e}")
            return None
    
    async def get_cluster_status(self) -> Optional[Dict[str, Any]]:
        """Get cluster status information."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return None
            
            # Get cluster status
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.ClusterManagerStub(channel)
            
            request = storage_pb2.GetClusterStatusRequest()
            response = await stub.GetClusterStatus(request)
            await channel.close()
            
            nodes = []
            for node_info in response.nodes:
                nodes.append({
                    "node_id": node_info.node_id,
                    "address": node_info.address,
                    "port": node_info.port,
                    "healthy": node_info.healthy,
                    "last_seen": node_info.last_seen,
                    "storage_used": node_info.storage_used,
                    "storage_available": node_info.storage_available
                })
            
            return {
                "nodes": nodes,
                "total_files": response.total_files,
                "total_size": response.total_size
            }
            
        except Exception as e:
            self.logger.error(f"Get cluster status error: {e}")
            return None
    
    async def rebalance_cluster(self) -> bool:
        """Trigger cluster rebalancing."""
        try:
            # Find healthy node
            node = await self._get_healthy_node()
            if not node:
                self.logger.error("No healthy nodes available")
                return False
            
            # Trigger rebalance
            channel = grpc.aio.insecure_channel(node)
            stub = storage_pb2_grpc.ClusterManagerStub(channel)
            
            request = storage_pb2.RebalanceRequest()
            response = await stub.Rebalance(request)
            await channel.close()
            
            if response.success:
                self.logger.info(f"Rebalancing completed: {response.files_moved} files moved")
                return True
            else:
                self.logger.error(f"Rebalancing failed: {response.message}")
                return False
                
        except Exception as e:
            self.logger.error(f"Rebalance error: {e}")
            return False
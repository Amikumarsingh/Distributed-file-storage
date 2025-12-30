"""Distributed file storage node server."""

import asyncio
import argparse
import uuid
import time
from concurrent import futures
from typing import Dict, List

import grpc
from proto import storage_pb2_grpc, storage_pb2

from dfs.core.consistent_hash import ConsistentHashRing, Node
from dfs.core.storage import StorageEngine
from dfs.core.replication import ReplicationManager
from dfs.utils.crypto import calculate_checksum, generate_file_id, verify_checksum
from dfs.utils.logging import setup_logging, get_logger


class FileStorageService(storage_pb2_grpc.FileStorageServicer):
    """gRPC service for file operations."""
    
    def __init__(self, node_server):
        self.node_server = node_server
        self.logger = get_logger("file_service")
    
    async def UploadFile(self, request, context):
        """Handle file upload requests."""
        try:
            # Verify checksum
            if not verify_checksum(request.data, request.checksum):
                return storage_pb2.UploadFileResponse(
                    success=False,
                    message="Checksum verification failed"
                )
            
            # Generate file ID
            file_id = generate_file_id(request.filename, request.data)
            
            # Store file locally
            success = self.node_server.storage.store_file(
                request.filename, file_id, request.data, 
                request.checksum, dict(request.metadata)
            )
            
            if not success:
                return storage_pb2.UploadFileResponse(
                    success=False,
                    message="Failed to store file locally"
                )
            
            # Replicate to other nodes
            replica_nodes = await self.node_server.replication_manager.replicate_file(
                request.filename, file_id, request.data, 
                request.checksum, dict(request.metadata)
            )
            
            self.logger.info(f"Uploaded file {request.filename} with {len(replica_nodes)} replicas")
            
            return storage_pb2.UploadFileResponse(
                success=True,
                message="File uploaded successfully",
                file_id=file_id,
                node_ids=replica_nodes
            )
            
        except Exception as e:
            self.logger.error(f"Upload failed: {e}")
            return storage_pb2.UploadFileResponse(
                success=False,
                message=f"Upload failed: {str(e)}"
            )
    
    async def DownloadFile(self, request, context):
        """Handle file download requests."""
        try:
            result = self.node_server.storage.retrieve_file(request.filename)
            
            if not result:
                return storage_pb2.DownloadFileResponse(
                    success=False,
                    message="File not found"
                )
            
            data, metadata = result
            
            return storage_pb2.DownloadFileResponse(
                success=True,
                message="File retrieved successfully",
                data=data,
                checksum=metadata.checksum,
                metadata=metadata.metadata
            )
            
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            return storage_pb2.DownloadFileResponse(
                success=False,
                message=f"Download failed: {str(e)}"
            )
    
    async def DeleteFile(self, request, context):
        """Handle file deletion requests."""
        try:
            success = self.node_server.storage.delete_file(request.filename)
            
            if success:
                return storage_pb2.DeleteFileResponse(
                    success=True,
                    message="File deleted successfully"
                )
            else:
                return storage_pb2.DeleteFileResponse(
                    success=False,
                    message="File not found"
                )
                
        except Exception as e:
            self.logger.error(f"Delete failed: {e}")
            return storage_pb2.DeleteFileResponse(
                success=False,
                message=f"Delete failed: {str(e)}"
            )
    
    async def ListFiles(self, request, context):
        """Handle file listing requests."""
        try:
            files = self.node_server.storage.list_files(request.prefix, request.limit or 100)
            
            file_infos = []
            for file_meta in files:
                file_info = storage_pb2.FileInfo(
                    filename=file_meta.filename,
                    size=file_meta.size,
                    checksum=file_meta.checksum,
                    created_at=int(file_meta.created_at),
                    modified_at=int(file_meta.modified_at),
                    version=file_meta.version,
                    replicas=file_meta.replicas,
                    metadata=file_meta.metadata
                )
                file_infos.append(file_info)
            
            return storage_pb2.ListFilesResponse(files=file_infos)
            
        except Exception as e:
            self.logger.error(f"List files failed: {e}")
            return storage_pb2.ListFilesResponse(files=[])
    
    async def GetFileInfo(self, request, context):
        """Handle file info requests."""
        try:
            file_meta = self.node_server.storage.get_file_info(request.filename)
            
            if not file_meta:
                return storage_pb2.GetFileInfoResponse(success=False)
            
            file_info = storage_pb2.FileInfo(
                filename=file_meta.filename,
                size=file_meta.size,
                checksum=file_meta.checksum,
                created_at=int(file_meta.created_at),
                modified_at=int(file_meta.modified_at),
                version=file_meta.version,
                replicas=file_meta.replicas,
                metadata=file_meta.metadata
            )
            
            return storage_pb2.GetFileInfoResponse(
                success=True,
                file_info=file_info
            )
            
        except Exception as e:
            self.logger.error(f"Get file info failed: {e}")
            return storage_pb2.GetFileInfoResponse(success=False)


class ClusterManagerService(storage_pb2_grpc.ClusterManagerServicer):
    """gRPC service for cluster management."""
    
    def __init__(self, node_server):
        self.node_server = node_server
        self.logger = get_logger("cluster_service")
    
    async def JoinCluster(self, request, context):
        """Handle node join requests."""
        try:
            node = Node(
                node_id=request.node_id,
                address=request.address,
                port=request.port
            )
            
            self.node_server.hash_ring.add_node(node)
            
            # Return existing nodes
            existing_nodes = []
            for existing_node in self.node_server.hash_ring.get_all_nodes():
                if existing_node.node_id != request.node_id:
                    node_info = storage_pb2.NodeInfo(
                        node_id=existing_node.node_id,
                        address=existing_node.address,
                        port=existing_node.port,
                        healthy=existing_node.healthy,
                        last_seen=int(time.time())
                    )
                    existing_nodes.append(node_info)
            
            self.logger.info(f"Node {request.node_id} joined cluster")
            
            return storage_pb2.JoinClusterResponse(
                success=True,
                message="Successfully joined cluster",
                existing_nodes=existing_nodes
            )
            
        except Exception as e:
            self.logger.error(f"Join cluster failed: {e}")
            return storage_pb2.JoinClusterResponse(
                success=False,
                message=f"Join failed: {str(e)}"
            )
    
    async def LeaveCluster(self, request, context):
        """Handle node leave requests."""
        try:
            self.node_server.hash_ring.remove_node(request.node_id)
            
            self.logger.info(f"Node {request.node_id} left cluster")
            
            return storage_pb2.LeaveClusterResponse(
                success=True,
                message="Successfully left cluster"
            )
            
        except Exception as e:
            self.logger.error(f"Leave cluster failed: {e}")
            return storage_pb2.LeaveClusterResponse(
                success=False,
                message=f"Leave failed: {str(e)}"
            )
    
    async def GetClusterStatus(self, request, context):
        """Handle cluster status requests."""
        try:
            nodes = []
            stats = self.node_server.storage.get_storage_stats()
            
            for node in self.node_server.hash_ring.get_all_nodes():
                node_info = storage_pb2.NodeInfo(
                    node_id=node.node_id,
                    address=node.address,
                    port=node.port,
                    healthy=node.healthy,
                    last_seen=int(time.time()),
                    storage_used=stats.get("total_size", 0),
                    storage_available=stats.get("available_space", 0)
                )
                nodes.append(node_info)
            
            return storage_pb2.GetClusterStatusResponse(
                nodes=nodes,
                total_files=stats.get("total_files", 0),
                total_size=stats.get("total_size", 0)
            )
            
        except Exception as e:
            self.logger.error(f"Get cluster status failed: {e}")
            return storage_pb2.GetClusterStatusResponse(nodes=[])
    
    async def Rebalance(self, request, context):
        """Handle cluster rebalancing requests."""
        try:
            # Trigger replication check and repair
            repair_stats = await self.node_server.replication_manager.check_and_repair_replicas()
            
            return storage_pb2.RebalanceResponse(
                success=True,
                message="Rebalancing completed",
                files_moved=repair_stats.get("repaired", 0)
            )
            
        except Exception as e:
            self.logger.error(f"Rebalance failed: {e}")
            return storage_pb2.RebalanceResponse(
                success=False,
                message=f"Rebalance failed: {str(e)}"
            )
    
    async def HealthCheck(self, request, context):
        """Handle health check requests."""
        try:
            stats = self.node_server.storage.get_storage_stats()
            repl_stats = self.node_server.replication_manager.get_replication_stats()
            
            metrics = {
                "total_files": str(stats.get("total_files", 0)),
                "total_size": str(stats.get("total_size", 0)),
                "available_space": str(stats.get("available_space", 0)),
                "under_replicated": str(repl_stats.get("under_replicated", 0)),
                "node_count": str(self.node_server.hash_ring.get_node_count())
            }
            
            return storage_pb2.HealthCheckResponse(
                healthy=True,
                status="healthy",
                metrics=metrics
            )
            
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return storage_pb2.HealthCheckResponse(
                healthy=False,
                status=f"unhealthy: {str(e)}"
            )


class NodeService(storage_pb2_grpc.NodeServiceServicer):
    """gRPC service for node-to-node communication."""
    
    def __init__(self, node_server):
        self.node_server = node_server
        self.logger = get_logger("node_service")
    
    async def StoreChunk(self, request, context):
        """Handle chunk storage requests from other nodes."""
        try:
            # For simplicity, treat chunks as complete files
            success = self.node_server.storage.store_file(
                request.file_id, request.file_id, request.data, request.checksum
            )
            
            return storage_pb2.StoreChunkResponse(
                success=success,
                message="Chunk stored successfully" if success else "Failed to store chunk"
            )
            
        except Exception as e:
            self.logger.error(f"Store chunk failed: {e}")
            return storage_pb2.StoreChunkResponse(
                success=False,
                message=f"Store chunk failed: {str(e)}"
            )
    
    async def RetrieveChunk(self, request, context):
        """Handle chunk retrieval requests from other nodes."""
        try:
            result = self.node_server.storage.retrieve_file(request.file_id)
            
            if not result:
                return storage_pb2.RetrieveChunkResponse(
                    success=False,
                    message="Chunk not found"
                )
            
            data, metadata = result
            
            return storage_pb2.RetrieveChunkResponse(
                success=True,
                message="Chunk retrieved successfully",
                data=data,
                checksum=metadata.checksum
            )
            
        except Exception as e:
            self.logger.error(f"Retrieve chunk failed: {e}")
            return storage_pb2.RetrieveChunkResponse(
                success=False,
                message=f"Retrieve chunk failed: {str(e)}"
            )
    
    async def DeleteChunk(self, request, context):
        """Handle chunk deletion requests from other nodes."""
        try:
            success = self.node_server.storage.delete_file(request.file_id)
            
            return storage_pb2.DeleteChunkResponse(
                success=success,
                message="Chunk deleted successfully" if success else "Chunk not found"
            )
            
        except Exception as e:
            self.logger.error(f"Delete chunk failed: {e}")
            return storage_pb2.DeleteChunkResponse(
                success=False,
                message=f"Delete chunk failed: {str(e)}"
            )
    
    async def ReplicateFile(self, request, context):
        """Handle file replication requests from other nodes."""
        try:
            success = self.node_server.storage.store_file(
                request.filename, request.file_id, request.data, 
                request.checksum, dict(request.metadata)
            )
            
            return storage_pb2.ReplicateFileResponse(
                success=success,
                message="File replicated successfully" if success else "Failed to replicate file"
            )
            
        except Exception as e:
            self.logger.error(f"Replicate file failed: {e}")
            return storage_pb2.ReplicateFileResponse(
                success=False,
                message=f"Replicate file failed: {str(e)}"
            )


class NodeServer:
    """Main node server class."""
    
    def __init__(self, node_id: str, address: str, port: int, data_dir: str, 
                 replication_factor: int = 3):
        self.node_id = node_id
        self.address = address
        self.port = port
        self.data_dir = data_dir
        self.replication_factor = replication_factor
        
        # Initialize components
        self.hash_ring = ConsistentHashRing()
        self.storage = StorageEngine(data_dir)
        self.replication_manager = ReplicationManager(
            self.hash_ring, self.storage, replication_factor
        )
        
        # Add self to hash ring
        self_node = Node(node_id, address, port)
        self.hash_ring.add_node(self_node)
        
        self.logger = setup_logging("INFO", node_id)
        self.server = None
    
    async def start(self, peers: List[str] = None):
        """Start the node server."""
        self.server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
        
        # Add services
        storage_pb2_grpc.add_FileStorageServicer_to_server(
            FileStorageService(self), self.server
        )
        storage_pb2_grpc.add_ClusterManagerServicer_to_server(
            ClusterManagerService(self), self.server
        )
        storage_pb2_grpc.add_NodeServiceServicer_to_server(
            NodeService(self), self.server
        )
        
        # Bind to port
        listen_addr = f"{self.address}:{self.port}"
        self.server.add_insecure_port(listen_addr)
        
        await self.server.start()
        self.logger.info(f"Node {self.node_id} started on {listen_addr}")
        
        # Join existing cluster if peers provided
        if peers:
            await self._join_existing_cluster(peers)
        
        # Start background tasks
        asyncio.create_task(self._health_check_loop())
        asyncio.create_task(self._replication_check_loop())
        
        await self.server.wait_for_termination()
    
    async def _join_existing_cluster(self, peers: List[str]):
        """Join an existing cluster by contacting peer nodes."""
        for peer in peers:
            try:
                channel = grpc.aio.insecure_channel(peer)
                stub = storage_pb2_grpc.ClusterManagerStub(channel)
                
                request = storage_pb2.JoinClusterRequest(
                    node_id=self.node_id,
                    address=self.address,
                    port=self.port
                )
                
                response = await stub.JoinCluster(request)
                await channel.close()
                
                if response.success:
                    # Add existing nodes to hash ring
                    for node_info in response.existing_nodes:
                        node = Node(
                            node_info.node_id,
                            node_info.address,
                            node_info.port,
                            node_info.healthy
                        )
                        self.hash_ring.add_node(node)
                    
                    self.logger.info(f"Successfully joined cluster via {peer}")
                    break
                    
            except Exception as e:
                self.logger.warning(f"Failed to join cluster via {peer}: {e}")
    
    async def _health_check_loop(self):
        """Periodic health check of cluster nodes."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                for node in self.hash_ring.get_all_nodes():
                    if node.node_id == self.node_id:
                        continue
                    
                    try:
                        channel = grpc.aio.insecure_channel(f"{node.address}:{node.port}")
                        stub = storage_pb2_grpc.ClusterManagerStub(channel)
                        
                        request = storage_pb2.HealthCheckRequest()
                        response = await asyncio.wait_for(
                            stub.HealthCheck(request), timeout=5.0
                        )
                        await channel.close()
                        
                        if response.healthy:
                            self.hash_ring.mark_node_healthy(node.node_id)
                        else:
                            self.hash_ring.mark_node_unhealthy(node.node_id)
                            
                    except Exception:
                        self.hash_ring.mark_node_unhealthy(node.node_id)
                        
            except Exception as e:
                self.logger.error(f"Health check loop error: {e}")
    
    async def _replication_check_loop(self):
        """Periodic replication check and repair."""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                await self.replication_manager.check_and_repair_replicas()
            except Exception as e:
                self.logger.error(f"Replication check loop error: {e}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Distributed File Storage Node")
    parser.add_argument("--port", type=int, default=8001, help="Port to listen on")
    parser.add_argument("--address", default="localhost", help="Address to bind to")
    parser.add_argument("--data-dir", default="./data", help="Data directory")
    parser.add_argument("--peers", nargs="*", help="Peer nodes to join")
    parser.add_argument("--replication-factor", type=int, default=3, 
                       help="Replication factor")
    
    args = parser.parse_args()
    
    # Generate unique node ID
    node_id = str(uuid.uuid4())[:8]
    
    # Create and start node server
    server = NodeServer(
        node_id=node_id,
        address=args.address,
        port=args.port,
        data_dir=args.data_dir,
        replication_factor=args.replication_factor
    )
    
    try:
        await server.start(args.peers)
    except KeyboardInterrupt:
        print("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
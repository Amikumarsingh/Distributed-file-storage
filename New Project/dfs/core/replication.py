"""Replication manager for fault tolerance."""

import asyncio
from typing import List, Dict, Set
from dfs.core.consistent_hash import Node, ConsistentHashRing
from dfs.core.storage import StorageEngine, FileMetadata
from dfs.utils.logging import get_logger
import grpc
from proto import storage_pb2_grpc, storage_pb2


class ReplicationManager:
    """Manages file replication across nodes."""
    
    def __init__(self, hash_ring: ConsistentHashRing, storage: StorageEngine, 
                 replication_factor: int = 3):
        self.hash_ring = hash_ring
        self.storage = storage
        self.replication_factor = replication_factor
        self.logger = get_logger("replication")
    
    async def replicate_file(self, filename: str, file_id: str, data: bytes, 
                           checksum: str, metadata: Dict[str, str] = None) -> List[str]:
        """Replicate file to multiple nodes."""
        target_nodes = self.hash_ring.get_nodes(filename, self.replication_factor)
        successful_replicas = []
        
        # Create replication tasks
        tasks = []
        for node in target_nodes:
            task = self._replicate_to_node(node, file_id, filename, data, checksum, metadata)
            tasks.append(task)
        
        # Execute replications concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error(f"Replication to {target_nodes[i]} failed: {result}")
            elif result:
                successful_replicas.append(target_nodes[i].node_id)
        
        self.logger.info(f"File {filename} replicated to {len(successful_replicas)} nodes")
        return successful_replicas
    
    async def _replicate_to_node(self, node: Node, file_id: str, filename: str, 
                               data: bytes, checksum: str, metadata: Dict[str, str]) -> bool:
        """Replicate file to a specific node."""
        try:
            channel = grpc.aio.insecure_channel(f"{node.address}:{node.port}")
            stub = storage_pb2_grpc.NodeServiceStub(channel)
            
            request = storage_pb2.ReplicateFileRequest(
                file_id=file_id,
                filename=filename,
                data=data,
                checksum=checksum,
                metadata=metadata or {}
            )
            
            response = await stub.ReplicateFile(request)
            await channel.close()
            
            return response.success
            
        except Exception as e:
            self.logger.error(f"Failed to replicate to {node}: {e}")
            return False
    
    async def check_and_repair_replicas(self) -> Dict[str, int]:
        """Check and repair under-replicated files."""
        repair_stats = {"checked": 0, "repaired": 0, "failed": 0}
        
        files = self.storage.list_files()
        
        for file_metadata in files:
            repair_stats["checked"] += 1
            
            # Get current replica count
            healthy_replicas = await self._get_healthy_replicas(file_metadata)
            
            if len(healthy_replicas) < self.replication_factor:
                self.logger.warning(
                    f"File {file_metadata.filename} under-replicated: "
                    f"{len(healthy_replicas)}/{self.replication_factor}"
                )
                
                # Attempt repair
                if await self._repair_file_replicas(file_metadata, healthy_replicas):
                    repair_stats["repaired"] += 1
                else:
                    repair_stats["failed"] += 1
        
        return repair_stats
    
    async def _get_healthy_replicas(self, file_metadata: FileMetadata) -> List[Node]:
        """Get list of healthy nodes containing the file."""
        healthy_replicas = []
        
        for node_id in file_metadata.replicas:
            node = self.hash_ring.nodes.get(node_id)
            if node and node.healthy:
                # Verify file exists on node
                if await self._verify_file_on_node(node, file_metadata.file_id):
                    healthy_replicas.append(node)
        
        return healthy_replicas
    
    async def _verify_file_on_node(self, node: Node, file_id: str) -> bool:
        """Verify that a file exists on a specific node."""
        try:
            channel = grpc.aio.insecure_channel(f"{node.address}:{node.port}")
            stub = storage_pb2_grpc.NodeServiceStub(channel)
            
            request = storage_pb2.RetrieveChunkRequest(
                file_id=file_id,
                chunk_id="0"  # For simplicity, treating whole file as one chunk
            )
            
            response = await stub.RetrieveChunk(request)
            await channel.close()
            
            return response.success
            
        except Exception:
            return False
    
    async def _repair_file_replicas(self, file_metadata: FileMetadata, 
                                  healthy_replicas: List[Node]) -> bool:
        """Repair under-replicated file."""
        try:
            if not healthy_replicas:
                self.logger.error(f"No healthy replicas found for {file_metadata.filename}")
                return False
            
            # Get file data from a healthy replica
            source_node = healthy_replicas[0]
            file_data = await self._retrieve_file_from_node(source_node, file_metadata.file_id)
            
            if not file_data:
                self.logger.error(f"Failed to retrieve {file_metadata.filename} from {source_node}")
                return False
            
            # Find additional nodes for replication
            needed_replicas = self.replication_factor - len(healthy_replicas)
            target_nodes = self.hash_ring.get_nodes(file_metadata.filename, 
                                                  self.replication_factor + needed_replicas)
            
            # Filter out nodes that already have the file
            existing_node_ids = {node.node_id for node in healthy_replicas}
            new_target_nodes = [node for node in target_nodes 
                              if node.node_id not in existing_node_ids][:needed_replicas]
            
            # Replicate to new nodes
            successful_replicas = []
            for node in new_target_nodes:
                if await self._replicate_to_node(node, file_metadata.file_id, 
                                               file_metadata.filename, file_data, 
                                               file_metadata.checksum, file_metadata.metadata):
                    successful_replicas.append(node.node_id)
            
            # Update replica list
            file_metadata.replicas.extend(successful_replicas)
            
            self.logger.info(f"Repaired {file_metadata.filename}: added {len(successful_replicas)} replicas")
            return len(successful_replicas) > 0
            
        except Exception as e:
            self.logger.error(f"Failed to repair {file_metadata.filename}: {e}")
            return False
    
    async def _retrieve_file_from_node(self, node: Node, file_id: str) -> bytes:
        """Retrieve file data from a specific node."""
        try:
            channel = grpc.aio.insecure_channel(f"{node.address}:{node.port}")
            stub = storage_pb2_grpc.NodeServiceStub(channel)
            
            request = storage_pb2.RetrieveChunkRequest(
                file_id=file_id,
                chunk_id="0"
            )
            
            response = await stub.RetrieveChunk(request)
            await channel.close()
            
            if response.success:
                return response.data
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve file from {node}: {e}")
            return None
    
    def get_replication_stats(self) -> Dict[str, int]:
        """Get replication statistics."""
        files = self.storage.list_files()
        stats = {
            "total_files": len(files),
            "under_replicated": 0,
            "properly_replicated": 0,
            "over_replicated": 0
        }
        
        for file_metadata in files:
            replica_count = len(file_metadata.replicas)
            if replica_count < self.replication_factor:
                stats["under_replicated"] += 1
            elif replica_count == self.replication_factor:
                stats["properly_replicated"] += 1
            else:
                stats["over_replicated"] += 1
        
        return stats
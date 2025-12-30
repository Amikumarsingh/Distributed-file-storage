"""Consistent hashing implementation for distributed file placement."""

import hashlib
import bisect
from typing import List, Dict, Set, Optional
from dataclasses import dataclass


@dataclass
class Node:
    """Represents a storage node in the cluster."""
    node_id: str
    address: str
    port: int
    healthy: bool = True
    
    def __str__(self) -> str:
        return f"{self.address}:{self.port}"


class ConsistentHashRing:
    """Consistent hash ring for distributed file placement."""
    
    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self.nodes: Dict[str, Node] = {}
    
    def _hash(self, key: str) -> int:
        """Generate hash value for a key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def add_node(self, node: Node) -> None:
        """Add a node to the hash ring."""
        self.nodes[node.node_id] = node
        
        for i in range(self.virtual_nodes):
            virtual_key = f"{node.node_id}:{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node.node_id
            bisect.insort(self.sorted_keys, hash_value)
    
    def remove_node(self, node_id: str) -> None:
        """Remove a node from the hash ring."""
        if node_id not in self.nodes:
            return
            
        del self.nodes[node_id]
        
        # Remove all virtual nodes
        keys_to_remove = []
        for hash_value, stored_node_id in self.ring.items():
            if stored_node_id == node_id:
                keys_to_remove.append(hash_value)
        
        for key in keys_to_remove:
            del self.ring[key]
            self.sorted_keys.remove(key)
    
    def get_node(self, key: str) -> Optional[Node]:
        """Get the primary node responsible for a key."""
        if not self.ring:
            return None
            
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        
        if idx == len(self.sorted_keys):
            idx = 0
            
        node_id = self.ring[self.sorted_keys[idx]]
        return self.nodes.get(node_id)
    
    def get_nodes(self, key: str, count: int) -> List[Node]:
        """Get multiple nodes for replication."""
        if not self.ring or count <= 0:
            return []
            
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        
        nodes = []
        seen_nodes = set()
        
        for _ in range(len(self.sorted_keys)):
            if idx >= len(self.sorted_keys):
                idx = 0
                
            node_id = self.ring[self.sorted_keys[idx]]
            
            if node_id not in seen_nodes and self.nodes[node_id].healthy:
                nodes.append(self.nodes[node_id])
                seen_nodes.add(node_id)
                
                if len(nodes) >= count:
                    break
                    
            idx += 1
        
        return nodes
    
    def get_all_nodes(self) -> List[Node]:
        """Get all nodes in the cluster."""
        return list(self.nodes.values())
    
    def get_healthy_nodes(self) -> List[Node]:
        """Get all healthy nodes in the cluster."""
        return [node for node in self.nodes.values() if node.healthy]
    
    def mark_node_unhealthy(self, node_id: str) -> None:
        """Mark a node as unhealthy."""
        if node_id in self.nodes:
            self.nodes[node_id].healthy = False
    
    def mark_node_healthy(self, node_id: str) -> None:
        """Mark a node as healthy."""
        if node_id in self.nodes:
            self.nodes[node_id].healthy = True
    
    def get_node_count(self) -> int:
        """Get total number of nodes."""
        return len(self.nodes)
    
    def get_healthy_node_count(self) -> int:
        """Get number of healthy nodes."""
        return len(self.get_healthy_nodes())
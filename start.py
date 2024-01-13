import threading
import socket
import time
import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from kazoo.client import KazooClient
import json

class Node:
  def __init__(self, ip, port, all_nodes, zookeeper_hosts):
        self.ip = ip
        self.port = port
        self.app = FastAPI()
        self.load = 0
        self.all_nodes = all_nodes
        self.cache = {}  # Simple cache
        self.cache_size = 1000

        # Initialize ZooKeeper client
        self.zk = KazooClient(hosts=zookeeper_hosts)
        self.zk.start()

        @self.app.get("/get/{key}")
        async def get(key: str):
            self.load += 1
            # Check if the key is in the cache
            if key in self.cache:
                self.load -= 1
                return self.cache[key]

            # Load balancing: redirect if load is high
            if self.load > 10:  # Threshold for load
                self.load -= 1
                return await self.redirect_request(key)

            # Retrieve from ZooKeeper
            if self.zk.exists(f"/kv_store/{key}"):
                data, _ = self.zk.get(f"/kv_store/{key}")
                value = json.loads(data.decode("utf-8"))
                if len(self.cache) >= self.cache_size:
                    self.cache.popitem()
                self.cache[key] = value
                self.load -= 1
                return value

            self.load -= 1
            return {"error": "Key not found"}

        @self.app.put("/put/{key}")
        async def put(key: str, value: str):
            # Store in ZooKeeper
            self.zk.ensure_path(f"/kv_store/{key}")
            self.zk.set(f"/kv_store/{key}", json.dumps(value).encode("utf-8"))
            return {"message": "Value stored successfully"}

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            if self.zk.exists(f"/kv_store/{key}"):
                self.zk.delete(f"/kv_store/{key}")
                return {"message": "Key deleted successfully"}
            return {"error": "Key not found"}

        @self.app.get("/health")
        async def health_check():
            return {"status": "alive"}

        async def redirect_request(self, key):
            node = self.select_node_for_redirection()
            if node:
                response = requests.get(f"http://{node['ip']}:{node['port']}/get/{key}")
                return response.json()
            else:
                raise HTTPException(status_code=500, detail="No suitable node found")
                                    
        # Select a node for redirection based on load
  def select_node_for_redirection(self):
        for node in self.all_nodes:
            if node['ip'] != self.ip or node['port'] != self.port:
                return node
        return None

  def start_server(self):
    uvicorn.run(self.app, host=self.ip, port=self.port)

class HeartbeatMonitor:
    def __init__(self, nodes):
        self.nodes = nodes  # List of nodes in the system

    def check_nodes(self):
        while True:
            for node in self.nodes:
                if not self.is_node_alive(node):
                    print(f"Node {node.ip}:{node.port} is down")
                else:
                    print(f"Node {node.ip}:{node.port} is up")
            time.sleep(5)

    def is_node_alive(self, node):
        try:
            response = requests.get(f"http://{node.ip}:{node.port}/health")
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False

# Example usage
zookeeper_hosts = "127.0.0.1:2181" # Replace with actual ZooKeeper hosts
nodes_info = [
{"ip": "127.0.1.1", "port": 8011},
{"ip": "127.0.1.2", "port": 8012}
]
nodes = [Node(node_info["ip"], node_info["port"], nodes_info, zookeeper_hosts) for node_info in nodes_info]
monitor = HeartbeatMonitor(nodes)


# Start monitoring in a separate thread
monitor_thread = threading.Thread(target=monitor.check_nodes)
monitor_thread.start()

# Start node servers
for node in nodes:
    threading.Thread(target=node.start_server).start()

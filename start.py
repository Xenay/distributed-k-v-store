import threading
import socket
import time
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Response, status
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import json

class Node:
  def __init__(self, ip, port, all_nodes, cassandra_hosts):
        self.ip = ip
        self.port = port
        self.app = FastAPI()
        self.load = 0
        self.all_nodes = all_nodes
        self.cache = {}  # Simple cache
        self.cache_size = 1000

        # Initialize ZooKeeper client
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        self.cluster = Cluster(cassandra_hosts, auth_provider=auth_provider)
        self.session = self.cluster.connect()
        try:
            self.session.execute("CREATE KEYSPACE IF NOT EXISTS kvstore WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '2'}")
            self.session.execute("CREATE TABLE IF NOT EXISTS kvstore.data (key text PRIMARY KEY, value text)")
        except Exception as e:
            print(f"Error creating keyspace or table: {e}")
        self.session.execute("USE kv_store;")
        

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
            result = self.session.execute(f"SELECT value FROM kvstore.data WHERE key = '{key}';").one()
            if result:
                value = result.value
                if len(self.cache) >= self.cache_size:
                    self.cache.popitem()
                self.cache[key] = value
                self.load -= 1
                return value

            self.load -= 1
            return {"error": "Key not found"}

        @self.app.post("/post/{key}")
        async def post(key: str, value: str):
        # Check if the key already exists
            select_query = SimpleStatement("SELECT value FROM kvstore.data WHERE key = %s")
            if self.session.execute(select_query, [key]).one():
                return Response(status_code=status.HTTP_409_CONFLICT,
                            content=f"Key '{key}' already exists.")

        # Insert the new key-value pair
            insert_query = SimpleStatement("INSERT INTO kvstore.data (key, value) VALUES (%s, %s)")
            self.session.execute(insert_query, (key, value))
            #self.update_cache(key, value)
            return {"message": "New key-value pair created successfully"}
        

        @self.app.put("/put/{key}")
        async def put(key: str, value: str):
            insert_query = SimpleStatement("INSERT INTO kvstore.data (key, value) VALUES (%s, %s)")
            self.session.execute(insert_query, (key, value))
            
            return {"message": "Value stored successfully"}

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            delete_query = SimpleStatement("DELETE FROM kvstore.data WHERE key = %s")
            self.session.execute(delete_query, [key])
            if key in self.cache:
                del self.cache[key]
            return {"message": "Key deleted successfully"}

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
cassandra_hosts = ['127.0.0.1'] # Replace with actual ZooKeeper hosts
nodes_info = [
{"ip": "127.0.1.1", "port": 8011},
{"ip": "127.0.1.2", "port": 8012}
]
nodes = [Node(node_info["ip"], node_info["port"], nodes_info, cassandra_hosts) for node_info in nodes_info]
monitor = HeartbeatMonitor(nodes)


# Start monitoring in a separate thread
monitor_thread = threading.Thread(target=monitor.check_nodes)
monitor_thread.start()

# Start node servers
for node in nodes:
    threading.Thread(target=node.start_server).start()

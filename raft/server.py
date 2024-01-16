import logging
import os
import random
import threading
import socket
import time
from fastapi.responses import JSONResponse
import requests
import uvicorn

from fastapi import FastAPI, HTTPException, Response, status
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from requests import Session
#import memcache
#import redis

app = FastAPI()

class CassandraSessionManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(CassandraSessionManager, cls).__new__(cls)
                cls._instance.cluster = None
                cls._instance.session = None
        return cls._instance

    def connect(self, hosts, port):
        if self.session is not None:
            return self.session

        try:
            auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
            self.cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth_provider)
            self.session = self.cluster.connect()
        except Exception as e:
            logging.error(f"Error connecting to Cassandra: {e}")
            raise

        return self.session

    def shutdown(self):
        if self.cluster is not None:
            self.cluster.shutdown()
        if self.session is not None:
            self.session.shutdown()

        self.cluster = None
        self.session = None
        

class Node:
  def __init__(self, ip, port, all_nodes, cassandra_hosts):
        self.ip = ip
        self.port = port
        
        self.active_requests = 0
        self.active_requests_lock = threading.Lock()
        self.http_session = Session()
        self.nodeIsAlive = True
        #self.redis_client = redis.Redis(host='localhost', port=6379, db=0)  # Adjust the host and port accordingly
        #self.memcached_client = memcache.Client(['127.0.0.1:11211'], debug=0)  # Adjust host and port if needed
        self.cassandra_hosts = cassandra_hosts  # Store the Cassandra hosts
        self.all_nodes = all_nodes
        #self.cache = {}  # Simple cache
        #self.cache_size = 1000
        self.cassandra_session_manager = CassandraSessionManager()
        self.state = "follower"
        self.last_heartbeat_time = time.time()
        self.election_timeout = random.randint(150, 300)
        
      
        
        app.get("/heartbeat")
        async def receive_heartbeat():
            if self.state != "leader":
                self.last_heartbeat_time = time.time()
                return {"status": "heartbeat received", "port": self.port}
            else:
                return {"error": "leader cannot receive heartbeats"}, 400
            
        app.get("/get/{key}")
        async def get(key: str):
            #cached_value = self.memcached_client.get(key)
            #cached_value = self.redis_client.get(key)
            #if cached_value:
                #return cached_value.decode('utf-8') 
            with self.active_requests_lock:
                self.active_requests += 1
                print(self.active_requests)
                # Load balancing: redirect if load is high
                #if self.should_redirect():  # Threshold for load
                self.active_requests -= 1
                print("to many requests, sending to:")
                        #return await self.redirect_request(key)
                try:
                            #self.memcached_client.set(key, value)
                            #self.redis_client.set(key, value)
                            self.active_requests -= 1
                except Exception as e:
                    print(f"Primary Cassandra instance failed: {e}")
                    # Switch to the secondary Cassandra instance
                    
                        
            with self.active_requests_lock:    
                self.active_requests -= 1
            return {"error": "Key not found"}

        app.post("/post/{key}")
        async def post(key: str, value: str):
        # Check if the key already exists
            #if not self.is_node_alive():
                #return await self.redirect_request(key)
            self.active_requests+=1
            self.active_requests-=1

        # Insert the new key-value pair
            
            self.active_requests-=1
            #self.update_cache(key, value)
            return {"message": "New key-value pair created successfully"}
        

        app.put("/put/{key}")
        async def put(key: str, value: str):
            #if not self.is_node_alive():
                #return await self.redirect_request(key)
            self.active_requests+=1
            
            self.active_requests-=1
            return {"message": "Value stored successfully"}

        app.delete("/delete/{key}")
        async def delete(key: str):
            self.active_requests+=1
            self.active_requests-=1

            return {"message": "Key deleted successfully"}
        

        app.get("/health")
        async def health_check():
            return {"status": "alive"}
        
        app.get("/shutdown")
        async def shutdown():
            # Perform any necessary cleanup here
            # ...

            # Shutdown the Uvicorn server
            uvicorn_server = getattr(self.app, 'server', None)
            if uvicorn_server:
                await uvicorn_server.shutdown()
            
            return {"status": "shutting down"}
        


  def start_server(self):
    self.server_thread = threading.Thread(target=uvicorn.run, args=(self.app,), kwargs={'host': self.ip, 'port': self.port, "log_level": "info"})
    self.server_thread.start()
  
    
  def check_if_leader_alive(self):
        return time.time() - self.last_heartbeat_time < 10  # Example timeout


class HeartbeatMonitor:
    def __init__(self, nodes):
        self.nodes = nodes
        self.node_status = {node: True for node in nodes}# List of nodes in the system

    def send_heartbeats(self):
        while True:
            for node in self.nodes:
                if node.state == "leader":
                    for follower in self.nodes:
                        if follower != node:
                            self.check_node_status(follower)
            time.sleep(3)  # Example heartbeat interval

    def check_node_status(self, node):
        try:
            response = requests.get(f"http://{node.ip}:{node.port}/heartbeat")
            if response.status_code == 200:
                print(f"Node {node.ip}:{node.port} is up")
            else:
                print(f"Node {node.ip}:{node.port} is down")
        except requests.exceptions.RequestException:
                print(f"Node {node.ip}:{node.port} is down")
                

# Example usage
cassandra_hosts = ['127.0.0.1'] # Replace with actual ZooKeeper hosts
nodes_info = [
{"ip": "127.0.1.1", "port": 8011},
{"ip": "127.0.1.2", "port": 8012},
{"ip": "127.0.1.3", "port": 8013},
]

nodes = [Node(node_info["ip"], node_info["port"], nodes_info, cassandra_hosts) for node_info in nodes_info]
nodes[0].state = "leader"
monitor = HeartbeatMonitor(nodes)

# Start monitoring in a separate thread
if nodes[0].state == "leader":
    monitor_thread = threading.Thread(target=monitor.send_heartbeats)
    monitor_thread.start()

# Start node servers
#for node in nodes:
  #threading.Thread(target=node.start_server).start()

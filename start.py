import threading
import socket
import time
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Response, status
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from requests import Session
#import memcache
#import redis





class Node:
  def __init__(self, ip, port, all_nodes, cassandra_hosts):
        self.ip = ip
        self.port = port
        self.app = FastAPI()
        self.active_requests = 0
        self.active_requests_lock = threading.Lock()
        self.http_session = Session()
        #self.redis_client = redis.Redis(host='localhost', port=6379, db=0)  # Adjust the host and port accordingly
        #self.memcached_client = memcache.Client(['127.0.0.1:11211'], debug=0)  # Adjust host and port if needed
        self.cassandra_hosts = cassandra_hosts  # Store the Cassandra hosts

        self.all_nodes = all_nodes
        #self.cache = {}  # Simple cache
        #self.cache_size = 1000
        
        
        
        # Initialize Cassandra client
        auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
        self.cluster = Cluster(cassandra_hosts, auth_provider=auth_provider)
        print(self.cluster.metadata, self.cluster.contact_points, self.cluster.port)
        self.session = self.cluster.connect()
        try:
            self.session.execute("CREATE KEYSPACE IF NOT EXISTS kvstore WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '2'}")
            self.session.execute("CREATE TABLE IF NOT EXISTS kvstore.data (key text PRIMARY KEY, value text)")
        except Exception as e:
            print(f"Error creating keyspace or table: {e}")
        self.session.execute("USE kvstore;")
        

        @self.app.get("/get/{key}")
        async def get(key: str):
            #cached_value = self.memcached_client.get(key)
            #cached_value = self.redis_client.get(key)
            #if cached_value:
                #return cached_value.decode('utf-8') 
            
            with self.active_requests_lock:
                self.active_requests += 1
                print(self.active_requests)
              
                             
                # Load balancing: redirect if load is high
                if self.should_redirect():  # Threshold for load
                        self.active_requests -= 1
                        print("to many requests, sending to:")
                        return await self.redirect_request(key)
                
                
                try:
                # Retrieve from Cassandra
                    result = self.session.execute(f"SELECT value FROM kvstore.data WHERE key = '{key}';").one()
                    if result:
                            value = result.value
                            #self.memcached_client.set(key, value)
                            #self.redis_client.set(key, value)
                            self.active_requests -= 1
                            
                            return value
                
                except Exception as e:
                    print(f"Primary Cassandra instance failed: {e}")
                    # Switch to the secondary Cassandra instance
                    self.switch_to_secondary_instance()
                    try:
                        # Retry the operation
                        result = self.session.execute(f"SELECT value FROM kvstore.data WHERE key = '{key}';").one()
                        if result:
                            return result.value
                    except Exception as e:
                        print(f"Secondary Cassandra instance also failed: {e}")
                        # Handle the case where both instances are down
                        return {"error": "Both Cassandra instances are down"}
                        
            with self.active_requests_lock:    
                self.active_requests -= 1
            return {"error": "Key not found"}

        @self.app.post("/post/{key}")
        async def post(key: str, value: str):
        # Check if the key already exists
            self.active_requests+=1
            select_query = SimpleStatement("SELECT value FROM kvstore.data WHERE key = %s")
            if self.session.execute(select_query, [key]).one():
                return Response(status_code=status.HTTP_409_CONFLICT,
                            content=f"Key '{key}' already exists.")

        # Insert the new key-value pair
            insert_query = SimpleStatement("INSERT INTO kvstore.data (key, value) VALUES (%s, %s)")
            self.session.execute(insert_query, (key, value))
            self.active_requests-=1
            #self.update_cache(key, value)
            return {"message": "New key-value pair created successfully"}
        

        @self.app.put("/put/{key}")
        async def put(key: str, value: str):
            self.active_requests+=1
            insert_query = SimpleStatement("INSERT INTO kvstore.data (key, value) VALUES (%s, %s)")
            self.session.execute(insert_query, (key, value))
            self.active_requests-=1
            return {"message": "Value stored successfully"}

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            self.active_requests+=1

            delete_query = SimpleStatement("DELETE FROM kvstore.data WHERE key = %s")
            self.session.execute(delete_query, [key])
            
            self.active_requests-=1

            return {"message": "Key deleted successfully"}

        @self.app.get("/health")
        async def health_check():
            return {"status": "alive"}
        

  def should_redirect(self):
            return self.active_requests > 100

  async def redirect_request(self, key):
    node = self.select_node_for_redirection()
    if node:
        response = self.http_session.get(f"http://{node['ip']}:{node['port']}/get/{key}")
        return response.json()
    else:
        raise HTTPException(status_code=500, detail="No suitable node found")
                                    
        # Select a node for redirection based on load
  def select_node_for_redirection(self):
    for node in self.all_nodes:
            if node['ip'] != self.ip or node['port'] != self.port:
                return node  # Redirect to the first different node
    return None

  def switch_to_secondary_instance(self):
    # Logic to switch to the secondary Cassandra instance
   
        #secondary_host = '127.0.0.1'  # Replace with the actual IP if different
        #secondary_port = 9043  # Port for the secondary instance
        self.cluster = Cluster(contact_points=["127.0.0.1"], port=9043, auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
        # Construct a new cluster object with the secondary instance
        #self.cluster = Cluster(contact_points=[secondary_host], port=secondary_port, auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))

        self.session = self.cluster.connect()
    

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
cashost1 = ['127.0.0.1']
cassandra_hosts = ['127.0.0.1'] # Replace with actual ZooKeeper hosts
nodes_info = [
{"ip": "127.0.1.1", "port": 8011},
{"ip": "127.0.1.2", "port": 8012},
{"ip": "127.0.1.3", "port": 8013},

]
nodes = [Node(node_info["ip"], node_info["port"], nodes_info, cassandra_hosts) for node_info in nodes_info]
monitor = HeartbeatMonitor(nodes)


# Start monitoring in a separate thread
monitor_thread = threading.Thread(target=monitor.check_nodes)
monitor_thread.start()

# Start node servers
for node in nodes:
    threading.Thread(target=node.start_server).start()

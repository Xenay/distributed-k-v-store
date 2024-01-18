import threading
import argparse
import logging
import os
import random
import sys
import threading
import socket
import time
from fastapi.responses import JSONResponse
import requests
import uvicorn
from typing import List, Dict
from fastapi import FastAPI, HTTPException, Response, status
from requests import Session

from raft.heartbeatMonitor import HeartbeatMonitor

class Node:
    def __init__(self, ip, port, all_nodes, cassandra_hosts, app):
        self.ip = ip
        self.port = port
        self.app = app
        self.active_requests = 0
        self.active_requests_lock = threading.Lock()
        self.http_session = Session()
        self.node_id = port
        self.nodeIsAlive = True
        #self.redis_client = redis.Redis(host='localhost', port=6379, db=0)  # Adjust the host and port accordingly
        #self.memcached_client = memcache.Client(['127.0.0.1:11211'], debug=0)  # Adjust host and port if needed
        self.cassandra_hosts = cassandra_hosts  # Store the Cassandra hosts
        self.all_nodes = all_nodes
        #self.cache = {}  # Simple cache
        #self.cache_size = 1000
        
        self.state = [state for state in all_nodes if state['port'] == self.port][0]['state']
        self.last_heartbeat_time = time.time()
        #election
        self.election_timeout = random.randint(450, 800) / 100
        self.term = 0  # Election term
        self.voted_for = None
        
        @self.app.get("/heartbeat")
        async def receive_heartbeat():
            
            self.last_heartbeat_time = time.time()
            
            
            return {"status": "heartbeat received", "port": self.port}
            
            
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

        @self.app.post("/post/{key}")
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
        

        @self.app.put("/put/{key}")
        async def put(key: str, value: str):
            #if not self.is_node_alive():
                #return await self.redirect_request(key)
            self.active_requests+=1
            
            self.active_requests-=1
            return {"message": "Value stored successfully"}

        @self.app.delete("/delete/{key}")
        async def delete(key: str):
            self.active_requests+=1
            self.active_requests-=1

            return {"message": "Key deleted successfully"}
        

        @self.app.get("/health")
        async def health_check():
            return {"status": "alive"}
        
        @self.app.get("/shutdown")
        async def shutdown():
            # Perform any necessary cleanup here
            # ...

            # Shutdown the Uvicorn server
            uvicorn_server = getattr(self.app, 'server', None)
            if uvicorn_server:
                await uvicorn_server.shutdown()
            
            return {"status": "shutting down"}
        
        @self.app.get("/vote")
        async def vote(candidate_id: str, term: int):
            if term > self.term and (self.voted_for is None or self.voted_for == candidate_id):
                self.voted_for = candidate_id
                self.term = term
                return {"vote_granted": True}
            return {"vote_granted": False}

        @self.app.get("/debug")
        async def debug():
            return {"state": self.state, "all nodes": self.all_nodes, }

    def start_server(self):
        uvicorn.run(self.app, host=self.ip, port=self.port, log_level="info")
  
    
    def start_election(self):
        self.term += 1
        self.state = "candidate"
        self.voted_for = self.port
        votes = 1

        for node in self.all_nodes:
            if node['port'] != self.port:
                try:
                    response = self.http_session.get(f"http://{node['ip']}:{node['port']}/vote",
                                                    params={"candidate_id": self.node_id, "term": self.term})
                    if response.status_code == 200 and response.json().get("vote_granted"):
                        votes += 1
                except requests.exceptions.RequestException as e:
                    print(f"Error contacting node {node['port']}: {e}")

        if votes > len(self.all_nodes) // 2:
            self.state = "leader"
            self.start_heartbeat()
            print(f"Elected as leader for term {self.term}")

    
    def check_if_leader_alive(self):
        
        print((time.time() - self.last_heartbeat_time))
        
        if (time.time() - self.last_heartbeat_time) > self.election_timeout and (self.state != "leader"):
            print("timeout")
            #self.start_election()
            return False
        return True
    
    def start_heartbeat(self):
        monitor = HeartbeatMonitor(self.all_nodes)
        monitor_thread = threading.Thread(target=monitor.send_heartbeats)
        monitor_thread.start()
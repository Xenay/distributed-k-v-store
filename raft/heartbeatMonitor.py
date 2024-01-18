import time
import requests


class HeartbeatMonitor():
    def __init__(self, nodes):
        self.nodes = nodes
        # List of nodes in the system

    def send_heartbeats(self):
        
        while True:
            
            for follower in self.nodes:
                if follower["state"] != 'leader':
                    self.check_node_status(follower)
            time.sleep(0.5)  # Example heartbeat interval

    def check_node_status(self, node):
        try:
            print(f"http://{node['ip']}:{node['port']}/heartbeat")
            response = requests.get(f"http://{node['ip']}:{node['port']}/heartbeat")
         
            if response.status_code == 200:
                
                print(f"Node {node['ip']}:{node['port']} is up")
            else:
                print(f"Node {node['ip']}:{node['port']} is down")
        except requests.exceptions.RequestException:
                print(f"Node {node['ip']}:{node['port']} is down")
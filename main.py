# Example usage
import sys
import threading
import time

from fastapi import FastAPI
from raft.node import Node

from raft.heartbeatMonitor import HeartbeatMonitor

app = FastAPI()
cassandra_hosts = ['127.0.0.1'] # Replace with actual ZooKeeper hosts
nodes_info = [
{"ip": "127.0.0.1", "port": 8011, "state": "leader"},
{"ip": "127.0.0.1", "port": 8012, "state": "follower"},
{"ip": "127.0.0.1", "port": 8013, "state": "follower"},
]


if "--port" in sys.argv:
        port_index = sys.argv.index("--port") + 1
        if port_index < len(sys.argv):
            port_number = int(sys.argv[port_index])
            
            
node = Node(ip = "127.0.0.1", port = port_number, all_nodes=nodes_info, cassandra_hosts=cassandra_hosts, app = app)
print(node.state)

#nodes = [Node(node_info["ip"], node_info["port"], nodes_info, cassandra_hosts) for node_info in nodes_info]
#nodes[0].state = "leader"
print(port_number)
if port_number == 8011:
    monitor = HeartbeatMonitor(nodes_info)
    monitor_thread = threading.Thread(target=monitor.send_heartbeats)
    monitor_thread.start()
# Start monitoring in a separa
def leader_check_loop():
    while True:
        if not node.check_if_leader_alive() and node.state != "leader":
            print("Leader is dead, starting election")
            node.start_election()
        time.sleep(0.5)

# Start the leader check loop in a separate thread
leader_check_thread = threading.Thread(target=leader_check_loop)
leader_check_thread.start()
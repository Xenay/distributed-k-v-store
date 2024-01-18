



# Step 1: Initialize State Machines
from raft2.RaftNode import RaftNode
from raft2.connections.RaftCluster import RaftCluster
from raft2.connections.implementations.ObjectRaftConnector import ObjectRaftConnector
from raft2.stateMachine.implementations.DictionaryStateMachine import DictionaryStateMachine

number_of_nodes = 4
state_machines = [DictionaryStateMachine() for _ in range(number_of_nodes)]

# Step 2: Create Raft Nodes
nodes = [RaftNode(node_id=i, state_machine=state_machines[i]) for i in range(number_of_nodes)]

# Step 3: Set Up Connections (using ObjectRaftConnector for simplicity)
connectors = [ObjectRaftConnector(node_id=node.node_id, node=node) for node in nodes]

# Step 4: Create and Configure the Cluster
cluster = RaftCluster()
for connector in connectors:
    cluster.add_node(connector)

# Step 5: Start the Nodes
for node in nodes:
    node.configure(cluster=cluster, election_timeout_ms=1000)  # Example timeout
    node.run()

# Your cluster should now be running

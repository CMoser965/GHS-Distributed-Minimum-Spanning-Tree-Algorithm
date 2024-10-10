class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.fragment_id = node_id  # Initially, each node is its own fragment
        self.neighbors = []  # Neighbors will be a list of (Node, edge_weight) tuples
        self.moe = None  # Minimum outgoing edge
        self.parent = None
        self.children = []
    
    def add_neighbor(self, neighbor_node, weight):
        self.neighbors.append((neighbor_node, weight))

    def find_moe(self):
        # Debugging: Print neighbors for each node
        print(f"Node {self.node_id} neighbors:")
        for neighbor, weight in self.neighbors:
            print(f"  Neighbor: {neighbor.node_id}, Fragment: {neighbor.fragment_id}, Weight: {weight}")

        # Find the minimum outgoing edge to another fragment
        moe = None
        for neighbor, weight in self.neighbors:
            if self.fragment_id != neighbor.fragment_id:  # Only consider edges to other fragments
                if moe is None or weight < moe[1]:
                    moe = (neighbor, weight)

        # If no valid MOE found (i.e., no connection to another fragment), return None
        if moe is None:
            return None
        
        self.moe = moe
        return self.moe

class GHS:
    def __init__(self, nodes):
        self.nodes = nodes  # List of Node objects
        self.fragments = {node.node_id: node for node in nodes}  # Tracks fragments
    
    def merge_fragments(self, node1, node2):
        # Check if node1 and node2 are connected by a direct edge before merging
        is_connected = False
        for neighbor, weight in node1.neighbors:
            if neighbor == node2:
                is_connected = True
                break
        
        if is_connected and node1.fragment_id != node2.fragment_id:
            new_fragment_id = min(node1.fragment_id, node2.fragment_id)
            old_fragment_id = max(node1.fragment_id, node2.fragment_id)

            # Debugging: Print the fragments being merged
            print(f"Merging fragment {old_fragment_id} into {new_fragment_id}")

            # Update fragment IDs for all nodes in the old fragment
            for node in self.nodes:
                if node.fragment_id == old_fragment_id:
                    node.fragment_id = new_fragment_id
        else:
            # Debugging: Print when merge is skipped due to no connection
            print(f"No merge: {node1.node_id} and {node2.node_id} are not connected")
    
    def run(self):
        # Continue until all nodes in connected components are in the same fragment
        any_merge = True
        while any_merge:  # Loop until no merges happen
            any_merge = False  # Track if any merging occurs

            # Step 1: Each node finds its minimum outgoing edge (MOE)
            for node in self.nodes:
                moe = node.find_moe()
                if moe:
                    # Debugging: Print the MOE for the node
                    print(f"Node {node.node_id} has MOE to {moe[0].node_id}")
                else:
                    # Debugging: Print when no MOE is found
                    print(f"Node {node.node_id} has no MOE (likely disconnected)")

                # Only proceed with merging if a MOE was found (i.e., node is connected)
                if moe:
                    self.merge_fragments(node, moe[0])
                    any_merge = True

            # Step 2: Clear MOEs for the next iteration
            for node in self.nodes:
                node.moe = None

# Example setup
node_a = Node('A')
node_b = Node('B')
node_c = Node('C')
node_d = Node('D')

# Add neighbors (add actual Node references)
node_a.add_neighbor(node_b, 1)
node_a.add_neighbor(node_c, 3)

node_b.add_neighbor(node_a, 1)
node_b.add_neighbor(node_c, 2)
node_b.add_neighbor(node_d, 4)

node_c.add_neighbor(node_a, 3)
node_c.add_neighbor(node_b, 2)

node_d.add_neighbor(node_b, 4)

# Create list of nodes and run the GHS algorithm
nodes = [node_a, node_b, node_c, node_d]
ghs = GHS(nodes)
ghs.run()

# Result: all nodes should be part of the same fragment after running the algorithm
for node in nodes:
    print(f"Node {node.node_id} is in fragment {node.fragment_id}")

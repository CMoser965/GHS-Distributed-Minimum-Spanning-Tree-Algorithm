import socket
import threading
import json
import time

MESSAGE_TYPES = {
    'MOE_REQUEST': 'moe_request',
    'MOE_RESPONSE': 'moe_response',
    'MERGE_REQUEST': 'merge_request',
    'MERGE_CONFIRMATION': 'merge_confirmation'
}


class DistributedNode:
    def __init__(self, node_id, neighbors, port):
        self.node_id = node_id
        self.fragment_id = node_id
        self.neighbors = neighbors  # List of (neighbor_address, port) tuples
        self.port = port  # Assign port to the node
        self.moe = None
        self.lock = threading.Lock()  # Protect state access
        self.fragment_changed = True  # Initialize with fragment changed
    
    def start(self):
        """ Start the node's server to listen for incoming messages. """
        threading.Thread(target=self.listen).start()

    def listen(self):
        """ Listen for incoming messages from neighbors. """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            # Allow reuse of the port
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(('localhost', self.port))  # Use the port assigned to this node
            server_socket.listen()

            print(f"Node {self.node_id} listening on port {self.port}")
            
            while True:
                conn, addr = server_socket.accept()
                with conn:
                    data = conn.recv(1024)
                    if data:
                        message = json.loads(data.decode('utf-8'))
                        self.handle_message(message)

    def handle_message(self, message):
        """ Handle incoming messages and process them. """
        msg_type = message['type']
        sender_id = message['sender_id']

        if msg_type == 'MOE_REQUEST':
            # Send back MOE_RESPONSE with fragment ID
            response = {
                'type': 'MOE_RESPONSE',
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            self.send_message(sender_id, response)

        elif msg_type == 'MOE_RESPONSE':
            # Process the response and check if it’s a valid MOE
            with self.lock:
                if message['fragment_id'] != self.fragment_id:
                    merge_request = {
                        'type': 'MERGE_REQUEST',
                        'sender_id': self.node_id
                    }
                    self.send_message(sender_id, merge_request)

        elif msg_type == 'MERGE_REQUEST':
            # If receiving a merge request, confirm and merge fragments
            with self.lock:
                if self.fragment_id != message['fragment_id']:
                    self.fragment_id = min(self.fragment_id, sender_id)
                    self.fragment_changed = True
                    # Propagate the change to all neighbors
                    self.propagate_fragment_change()

                confirmation = {
                    'type': 'MERGE_CONFIRMATION',
                    'sender_id': self.node_id
                }
                self.send_message(sender_id, confirmation)

        elif msg_type == 'MERGE_CONFIRMATION':
            # When receiving a merge confirmation, update fragment ID
            with self.lock:
                if self.fragment_id != message['fragment_id']:
                    self.fragment_id = min(self.fragment_id, sender_id)
                    self.fragment_changed = True
                    # Propagate the change to all neighbors
                    self.propagate_fragment_change()

        elif msg_type == 'FRAGMENT_UPDATE':
            # When receiving a fragment update, ensure the fragment is updated
            with self.lock:
                if self.fragment_id != message['fragment_id']:
                    print(f"Node {self.node_id} updating to new fragment ID: {message['fragment_id']}")
                    self.fragment_id = message['fragment_id']
                    self.fragment_changed = True
                    # Propagate further if needed
                    self.propagate_fragment_change()

    def propagate_fragment_change(self):
        """ Propagate the fragment change to all neighbors. """
        print(f"Node {self.node_id} propagating fragment change. New fragment ID: {self.fragment_id}")
        for neighbor_id, (neighbor_address, port) in self.neighbors.items():
            update_message = {
                'type': 'FRAGMENT_UPDATE',
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            self.send_message(neighbor_id, update_message)

    def send_message(self, neighbor_id, message):
        """ Send a message to a neighbor. """
        neighbor_address, port = self.neighbors[neighbor_id]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)  # Set a timeout of 5 seconds for the connection
            try:
                print(f"Node {self.node_id} sending message to {neighbor_id} on port {port}")
                s.connect((neighbor_address, port))
                s.sendall(json.dumps(message).encode('utf-8'))
                print(f"Node {self.node_id} successfully sent message to {neighbor_id}")
            except socket.timeout:
                print(f"Node {self.node_id} failed to send message to {neighbor_id}: Connection timed out.")

    def find_moe(self):
        """ Find the minimum outgoing edge. Sends MOE_REQUEST to all neighbors only if the fragment has changed. """
        if self.moe is None or self.fragment_changed:
            print(f"Node {self.node_id} sending MOE requests to neighbors...")
            for neighbor_id, (neighbor_address, port) in self.neighbors.items():
                moe_request = {
                    'type': MESSAGE_TYPES['MOE_REQUEST'],
                    'sender_id': self.node_id
                }
                self.send_message(neighbor_id, moe_request)
            self.fragment_changed = False  # Reset the flag after sending MOE requests
    
    def check_termination(self):
        """ Send a termination check message to all neighbors. """
        print(f"Node {self.node_id} checking for termination...")
        for neighbor_id, (neighbor_address, port) in self.neighbors.items():
            termination_request = {
                'type': 'CHECK_TERMINATION',
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            self.send_message(neighbor_id, termination_request)



def check_termination(nodes):
    # Periodically check if all nodes are in the same fragment
    while True:
        all_same_fragment = True
        for node in nodes:
            node.check_termination()
            time.sleep(1)  # Allow some time for responses
            if node.fragment_changed:
                all_same_fragment = False

        if all_same_fragment:
            print("All nodes are in the same fragment. Terminating...")
            break

if __name__ == '__main__':
    node_a = DistributedNode('A', {'B': ('localhost', 5001), 'C': ('localhost', 5002)}, 5000)
    node_b = DistributedNode('B', {'A': ('localhost', 5000), 'C': ('localhost', 5002), 'D': ('localhost', 5003)}, 5001)
    node_c = DistributedNode('C', {'A': ('localhost', 5000), 'B': ('localhost', 5001)}, 5002)
    node_d = DistributedNode('D', {'B': ('localhost', 5001)}, 5003)

    # Start each node (listening on respective ports)
    node_a.start()
    node_b.start()
    node_c.start()
    node_d.start()

    # Trigger each node to start finding MOE after startup
    print("Starting MOE requests...")
    node_a.find_moe()
    node_b.find_moe()
    node_c.find_moe()
    node_d.find_moe()

    check_termination([node_a, node_b, node_c, node_d])
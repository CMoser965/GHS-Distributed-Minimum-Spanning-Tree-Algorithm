import asyncio
from libp2p import generate_new_rsa_identity, new_host
from libp2p.network.stream.net_stream import INetStream

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.host = None
        self.peers = {}
        self.fragment_id = node_id  # Each node starts in its own fragment
        self.leader = node_id  # Initially, each node is its own leader
        self.min_outgoing_edge = None  # Minimum outgoing edge for GHS
        self.neighbors = []  # List of neighbor peer IDs

    def start_node(self):
        """ Initialize the node and start the libp2p host """
        key_pair = generate_new_rsa_identity()
        self.host = new_host(key_pair)

        # Make the host listen on a valid address (localhost and random available TCP port)
        addr = Multiaddr("/ip4/127.0.0.1/tcp/0")
        self.host.get_network().listen(addr)
        
        print(f"Node {self.node_id} started with peer ID {self.host.get_id()} and listening on:")

        # Print the addresses where the node is listening
        for addr in self.host.get_addrs():
            print(f"  {addr}")

        # Set stream handler for incoming connections
        self.host.set_stream_handler("/ghs/1.0.0", self.handle_stream)

    async def handle_stream(self, stream: INetStream):
        """ Handle incoming messages on the stream """
        async for message in stream:
            msg_str = message.decode('utf-8')
            print(f"Node {self.node_id} received: {msg_str}")
            await self.process_message(msg_str)

    async def process_message(self, message):
        """ Process incoming GHS messages """
        message_type, data = message.split(":")
        if message_type == "Test":
            await self.handle_test_message(data)
        elif message_type == "Report":
            await self.handle_report_message(data)
        elif message_type == "Merge":
            await self.handle_merge_message(data)

    async def send_message(self, peer_id: str, message: str):
        """ Send a message to a peer """
        try:
            # Get peer info and send message using new stream
            stream = await self.host.new_stream(peer_id, ["/ghs/1.0.0"])
            await stream.write(message.encode('utf-8'))
            print(f"Node {self.node_id} sent message: {message} to {peer_id}")
        except Exception as e:
            print(f"Failed to send message to {peer_id}: {e}")

    async def connect_to_peer(self, peer_id: str, peer_addrs):
        """ Connect to a peer and add to the peer list """
        try:
            # Connect to the peer using its peer ID and addresses
            await self.host.connect(peer_id, peer_addrs)
            self.peers[peer_id] = peer_addrs
            print(f"Node {self.node_id} connected to peer {peer_id}")
        except Exception as e:
            print(f"Failed to connect to peer {peer_id}: {e}")

    async def handle_test_message(self, data):
        """ Handle 'Test' message: Check fragment membership """
        sender_id, fragment_id = data.split(",")
        if self.fragment_id == fragment_id:
            print(f"Node {self.node_id}: Sender {sender_id} is in the same fragment.")
        else:
            print(f"Node {self.node_id}: Sender {sender_id} is in a different fragment.")

    async def handle_report_message(self, data):
        """ Handle 'Report' message: Update minimum outgoing edge """
        sender_id, outgoing_edge = data.split(",")
        outgoing_edge = int(outgoing_edge)
        if self.min_outgoing_edge is None or outgoing_edge < self.min_outgoing_edge:
            self.min_outgoing_edge = outgoing_edge
            print(f"Node {self.node_id}: Updated minimum outgoing edge to {self.min_outgoing_edge}")

    async def handle_merge_message(self, data):
        """ Handle 'Merge' message: Merge two fragments """
        sender_id, new_fragment_id = data.split(",")
        self.fragment_id = new_fragment_id
        print(f"Node {self.node_id}: Merged into fragment {new_fragment_id}")

    async def run_ghs_protocol(self):
        """ Main GHS protocol loop for each node """
        # Initiate GHS process by sending 'Test' messages to neighbors
        for neighbor_id in self.neighbors:
            await self.send_message(neighbor_id, f"Test:{self.node_id},{self.fragment_id}")


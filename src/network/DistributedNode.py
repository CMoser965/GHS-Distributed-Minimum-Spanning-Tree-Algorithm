import asyncio
import json

MESSAGE_TYPES = {
    'MOE_REQUEST': 'moe_request',
    'MOE_RESPONSE': 'moe_response',
    'MERGE_REQUEST': 'merge_request',
    'MERGE_CONFIRMATION': 'merge_confirmation',
    'FRAGMENT_UPDATE': 'fragment_update',
    'CHECK_TERMINATION': 'check_termination'
}

class DistributedNode:
    def __init__(self, node_id, neighbors, port):
        self.node_id = node_id
        self.fragment_id = node_id
        self.neighbors = neighbors
        self.port = port
        self.moe = None
        self.fragment_changed = True

    async def start(self):
        """ Start the node's server to listen for incoming messages. """
        print(f"Node {self.node_id} starting...")

        # Start the listening server as a background task
        asyncio.create_task(self.listen())
        
        print(f"Node {self.node_id} started listening.")

    async def listen(self):
        """ Listen for incoming messages from neighbors. """
        try:
            server_socket = await asyncio.start_server(self.handle_connection, 'localhost', self.port)
            print(f"Node {self.node_id} listening on port {self.port}")
            async with server_socket:
                await server_socket.serve_forever()
        except Exception as e:
            print(f"Error in node {self.node_id} while listening: {e}")

    async def handle_connection(self, reader, writer):
        """ Handle incoming connections. """
        try:
            print(f"Node {self.node_id} handling connection...")
            data = await reader.read(1024)
            if data:
                message = json.loads(data.decode('utf-8'))
                print(f"Node {self.node_id} received message: {message}")
                await self.handle_message(message)
        except Exception as e:
            print(f"Error handling connection on node {self.node_id}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def handle_message(self, message):
        """ Handle incoming messages and process them asynchronously. """
        msg_type = message['type']
        sender_id = message['sender_id']

        print(f"Node {self.node_id} handling message: {message}")

        if msg_type == 'MOE_REQUEST':
            response = {
                'type': 'MOE_RESPONSE',
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            await self.send_message(sender_id, response)

        elif msg_type == 'MOE_RESPONSE':
            if message['fragment_id'] != self.fragment_id:
                merge_request = {
                    'type': 'MERGE_REQUEST',
                    'sender_id': self.node_id
                }
                await self.send_message(sender_id, merge_request)

        elif msg_type == 'MERGE_REQUEST':
            if self.fragment_id != message['fragment_id']:
                self.fragment_id = min(self.fragment_id, sender_id)
                self.fragment_changed = True
                await self.propagate_fragment_change()

            confirmation = {
                'type': 'MERGE_CONFIRMATION',
                'sender_id': self.node_id
            }
            await self.send_message(sender_id, confirmation)

        elif msg_type == 'MERGE_CONFIRMATION':
            if self.fragment_id != message['fragment_id']:
                self.fragment_id = min(self.fragment_id, sender_id)
                self.fragment_changed = True
                await self.propagate_fragment_change()

        elif msg_type == 'FRAGMENT_UPDATE':
            if self.fragment_id != message['fragment_id']:
                print(f"Node {self.node_id} updating to new fragment ID: {message['fragment_id']}")
                self.fragment_id = message['fragment_id']
                self.fragment_changed = True
                await self.propagate_fragment_change()

    async def propagate_fragment_change(self):
        """ Propagate the fragment change to all neighbors asynchronously. """
        print(f"Node {self.node_id} propagating fragment change. New fragment ID: {self.fragment_id}")
        for neighbor_id, (neighbor_address, port) in self.neighbors.items():
            update_message = {
                'type': 'FRAGMENT_UPDATE',
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            await self.send_message(neighbor_id, update_message)

    async def send_message(self, neighbor_id, message):
        """ Send a message to a neighbor. """
        neighbor_address, port = self.neighbors[neighbor_id]
        try:
            print(f"Node {self.node_id} sending message to {neighbor_id} on port {port}")
            reader, writer = await asyncio.open_connection(neighbor_address, port)
            writer.write(json.dumps(message).encode('utf-8'))
            await writer.drain()
            print(f"Node {self.node_id} successfully sent message to {neighbor_id}")
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            print(f"Error sending message from node {self.node_id} to {neighbor_id}: {e}")

    async def find_moe(self):
        """ Find the minimum outgoing edge. Sends MOE_REQUEST to all neighbors only if the fragment has changed. """
        if self.moe is None or self.fragment_changed:
            print(f"Node {self.node_id} sending MOE requests to neighbors...")
            for neighbor_id, (neighbor_address, port) in self.neighbors.items():
                moe_request = {
                    'type': MESSAGE_TYPES['MOE_REQUEST'],
                    'sender_id': self.node_id
                }
                await self.send_message(neighbor_id, moe_request)
            self.fragment_changed = False  # Reset the flag after sending MOE requests


    
    async def check_termination(self):
        """ Send a termination check message to all neighbors asynchronously. """
        print(f"Node {self.node_id} checking for termination...")
        for neighbor_id, (neighbor_address, port) in self.neighbors.items():
            termination_request = {
                'type': MESSAGE_TYPES['CHECK_TERMINATION'],
                'sender_id': self.node_id,
                'fragment_id': self.fragment_id
            }
            await self.send_message(neighbor_id, termination_request)


async def check_termination(nodes):
    """ Check if all nodes have reached the same fragment and simulate merging. """
    fragment_id = None

    while True:
        all_same_fragment = True

        # Simulate fragment merging
        for node in nodes:
            if fragment_id is None:
                fragment_id = node.fragment_id
            elif node.fragment_id != fragment_id:
                print(f"Node {node.node_id} has a different fragment: {node.fragment_id}")
                node.fragment_id = fragment_id  # Simulate merging

        # Check if all nodes now have the same fragment
        for node in nodes:
            await node.check_termination()
            if node.fragment_id != fragment_id:
                all_same_fragment = False
                break

        if all_same_fragment:
            print("All nodes are in the same fragment. Terminating...")
            break
        
        await asyncio.sleep(1)  # Wait before the next check



async def main():
    node_a = DistributedNode('A', {'B': ('localhost', 5001), 'C': ('localhost', 5002)}, 5000)
    node_b = DistributedNode('B', {'A': ('localhost', 5000), 'C': ('localhost', 5002), 'D': ('localhost', 5003)}, 5001)
    node_c = DistributedNode('C', {'A': ('localhost', 5000), 'B': ('localhost', 5001)}, 5002)
    node_d = DistributedNode('D', {'B': ('localhost', 5001)}, 5003)

    # Start all nodes
    await asyncio.gather(node_a.start(), node_b.start(), node_c.start(), node_d.start())

    # Wait for nodes to fully start before proceeding
    await asyncio.sleep(1)

    # Trigger the GHS algorithm
    print("Starting MOE requests...")
    await asyncio.gather(node_a.find_moe(), node_b.find_moe(), node_c.find_moe(), node_d.find_moe())

    # Check for termination
    await check_termination([node_a, node_b, node_c, node_d])

async def simple_test():
    node_a = DistributedNode('A', {'B': ('localhost', 5001)}, 5000)
    node_b = DistributedNode('B', {'A': ('localhost', 5000)}, 5001)

    # Start both nodes
    await asyncio.gather(
        node_a.start(),
        node_b.start()
    )

    await asyncio.sleep(1)  # Allow time for the servers to start

    # Simulate simple message exchange
    await node_a.send_message('B', {'type': 'TEST', 'sender_id': 'A'})
    await asyncio.sleep(1)
    await node_b.send_message('A', {'type': 'TEST', 'sender_id': 'B'})
    
    await asyncio.sleep(2)

    # Check for termination
    await check_termination([node_a, node_b])

if __name__ == '__main__':
    try:
        asyncio.run(main())
        # asyncio.run(simple_test())
    except KeyboardInterrupt:
        print("")
        print("Exiting gracefully...")
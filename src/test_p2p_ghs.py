import asyncio
from libp2p.peer.peerinfo import PeerInfo
from multiaddr import Multiaddr
from Node import Node

async def test_multiple_nodes():
    """ Initialize and run multiple P2P nodes """

    # Create nodes
    node1 = Node(node_id=1)
    node2 = Node(node_id=2)
    node3 = Node(node_id=3)

    # Start nodes
    node1.start_node()
    node2.start_node()
    node3.start_node()

    # Debug: Print the addresses for each node to ensure they're correct
    print("Node 1 addresses: ", [str(addr) for addr in node1.host.get_addrs()])
    print("Node 2 addresses: ", [str(addr) for addr in node2.host.get_addrs()])
    print("Node 3 addresses: ", [str(addr) for addr in node3.host.get_addrs()])

    # Manually set neighbors (for testing)
    node1.neighbors = [node2.host.get_id().to_base58(), node3.host.get_id().to_base58()]
    node2.neighbors = [node1.host.get_id().to_base58()]
    node3.neighbors = [node1.host.get_id().to_base58()]

    # Create PeerInfo for each node, converting the addresses to Multiaddr format
    node1_info = PeerInfo(node1.host.get_id(), [Multiaddr(str(addr)) for addr in node1.host.get_addrs()])
    node2_info = PeerInfo(node2.host.get_id(), [Multiaddr(str(addr)) for addr in node2.host.get_addrs()])
    node3_info = PeerInfo(node3.host.get_id(), [Multiaddr(str(addr)) for addr in node3.host.get_addrs()])

    # Step 1: Manually connect nodes using PeerInfo objects
    await node1.host.connect(node2_info)
    await node1.host.connect(node3_info)
    await node2.host.connect(node1_info)
    await node3.host.connect(node1_info)

    # Step 2: Run GHS protocol on all nodes
    await asyncio.gather(
        node1.run_ghs_protocol(),
        node2.run_ghs_protocol(),
        node3.run_ghs_protocol()
    )

    # Simulate running the network for a while
    await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(test_multiple_nodes())

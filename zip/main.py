import time
import os
import sys
import networkx as nx
import asyncio
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from network.DistributedNode import DistributedNode, message_count  # Import global message_count

async def run_ghs_algorithm(subgraph, base_port):
    global message_count  # Declare global to track the count
    distributed_nodes = {}

    # Create the nodes and set up their neighbors within the subgraph
    for node in subgraph.nodes():
        neighbors = {neighbor: ('localhost', base_port + neighbor) for neighbor in subgraph.neighbors(node)}
        distributed_nodes[node] = DistributedNode(node, neighbors, base_port + node)

    # Print the number of nodes in the current subgraph
    print(f"Subgraph has {len(subgraph.nodes())} nodes")
    print(f"Subgraph has {len(subgraph.edges())} edges")

    # Start all nodes asynchronously
    tasks = [node.start() for node in distributed_nodes.values()]
    await asyncio.gather(*tasks)

    # Wait a bit for all nodes to initialize properly
    await asyncio.sleep(2)

    start_time = time.time()

    # Run the GHS algorithm (find minimum outgoing edges)
    moe_tasks = [node.find_moe() for node in distributed_nodes.values()]
    await asyncio.gather(*moe_tasks)

    # Check for termination
    await asyncio.sleep(1)  # Allow time for messages to propagate
    for node in distributed_nodes.values():
        await node.check_termination()

    end_time = time.time()

    # Return message_count and processing time
    return message_count, (end_time - start_time)

async def runNPartitions_async(subgraph, base_port):
    """ Asynchronous wrapper to run partitions and collect message count. """
    return await run_ghs_algorithm(subgraph, base_port)

def partition_graph_random(G, num_partitions):
    """ Partition graph G into num_partitions subgraphs using random assignment. """
    nodes = list(G.nodes())
    random.shuffle(nodes)

    partitions = [[] for _ in range(num_partitions)]
    for i, node in enumerate(nodes):
        partitions[i % num_partitions].append(node)

    subgraphs = [G.subgraph(partition).copy() for partition in partitions]
    return subgraphs

def runNPartitions(G, num_partitions: int):
    global message_count
    subgraphs = partition_graph_random(G, num_partitions)

    total_message_count = 0

    # Measure total start time
    total_start_time = time.time()

    # Now run GHS on each subgraph
    for i, subgraph in enumerate(subgraphs):
        # Reset message count for each partition run
        message_count = 0
        message_count, processing_time = asyncio.run(runNPartitions_async(subgraph, base_port=5000 + i * 1000))
        total_message_count += message_count
        print(f"Processing time for subgraph {i}: {processing_time:.4f} seconds")

    # Measure total end time
    total_end_time = time.time()

    # Output the total messages exchanged
    print(f"Total messages exchanged: {total_message_count}")

    # Return total message count and total time
    return total_message_count, total_end_time - total_start_time

if __name__ == '__main__':
    # Load the entire graph
    G = nx.read_edgelist("./dataset/p2p-Gnutella08.txt", nodetype=int)
    metrics = {}
    partitions = 30
    metrics[partitions] = runNPartitions(G, partitions)

    print(f"METRICS:")
    for key, val in metrics.items():
        print(f"{key}:\t{val}")

import time
import os
import sys
import networkx as nx
import asyncio
import random

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.DistributedNode import DistributedNode

def partition_graph_random(G, num_partitions):
    """ Partition graph G into num_partitions subgraphs using random assignment. """
    nodes = list(G.nodes())
    random.shuffle(nodes)
    
    partitions = [[] for _ in range(num_partitions)]
    for i, node in enumerate(nodes):
        partitions[i % num_partitions].append(node)

    subgraphs = [G.subgraph(partition).copy() for partition in partitions]
    return subgraphs

async def run_ghs_algorithm(subgraph, base_port):
    distributed_nodes = {}

    # Create the nodes and set up their neighbors within the subgraph
    for node in subgraph.nodes():
        neighbors = {neighbor: ('localhost', base_port + neighbor) for neighbor in subgraph.neighbors(node)}
        distributed_nodes[node] = DistributedNode(node, neighbors, base_port + node)

    # Start all nodes asynchronously
    tasks = [node.start() for node in distributed_nodes.values()]
    await asyncio.gather(*tasks)

    print(f"Starting GHS algorithm on fragment with {subgraph.number_of_nodes()} nodes.")
    start_time = time.time()

    # Run the GHS algorithm (find minimum outgoing edges)
    moe_tasks = [node.find_moe() for node in distributed_nodes.values()]
    await asyncio.gather(*moe_tasks)

    # Measure the processing time for this fragment
    end_time = time.time()
    print(f"Processing time for fragment: {end_time - start_time} seconds")

    # Check for termination
    await asyncio.sleep(1)  # Allow time for messages to propagate
    for node in distributed_nodes.values():
        await node.check_termination()

if __name__ == '__main__':
    # Load the entire graph
    G = nx.read_edgelist("./dataset/p2p-Gnutella08.txt", nodetype=int)
    num_partitions = 10  # Define number of fragments
    subgraphs = partition_graph_random(G, num_partitions)

    # Measure total start time
    total_start_time = time.time()

    # Now run GHS on each subgraph
    for i, subgraph in enumerate(subgraphs):
        print(f"Running GHS on fragment {i} with {subgraph.number_of_nodes()} nodes.")
        asyncio.run(run_ghs_algorithm(subgraph, base_port=5000 + i * 1000))

    # Measure total end time
    total_end_time = time.time()

    # Print total processing time for all fragments
    print(f"Total processing time for all fragments: {total_end_time - total_start_time} seconds")

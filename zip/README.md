### README

# Distributed GHS Algorithm Simulation

## Overview

This project is a Python-based implementation of the Gallager-Humblet-Spira (GHS) distributed algorithm for constructing a minimum spanning tree (MST) in a weighted undirected graph. The algorithm simulates a distributed environment using message passing between nodes, where each node is a process communicating asynchronously with its neighbors.

### Files

- **`main.py`**: The main file that handles the partitioning of the graph, initializes the nodes, and runs the GHS algorithm on each partition.
- **`DistributedNode.py`**: Defines the `DistributedNode` class, which represents a node in the distributed system. It handles message passing, fragment updates, and termination checks for the GHS algorithm.

## Setup

### Prerequisites

- Python 3.6 or later
- Required libraries:
  - `networkx`: For graph manipulation and partitioning.
  - `asyncio`: To simulate asynchronous message passing.
  - `json`: For message serialization and deserialization.

You can install the required packages using:
```bash
pip install networkx
```
How It Works

    Graph Partitioning: The graph is partitioned into subgraphs based on the number of partitions specified. Each partition becomes its own subgraph where the GHS algorithm is run independently.

    Distributed Node Setup: Each node is represented by a DistributedNode instance, and the nodes communicate with their neighbors by passing messages asynchronously.

    GHS Algorithm: The run_ghs_algorithm function is responsible for running the GHS algorithm on each partition, including the process of finding the minimum outgoing edge (MOE) and propagating fragment changes.

    Message Passing: Nodes exchange messages such as MOE requests, MOE responses, and fragment update messages to construct the MST. The total number of messages sent during the process is tracked globally.

Running the Code
Graph Input

The graph file (`p2p-Gnutella08.txt`) is read using NetworkX. It must be in the form of an edge list, where each line represents an edge between two nodes.
To run the algorithm:
```bash
python3 main.py
```

This script loads the graph, partitions it, and runs the GHS algorithm on each partition. The results, including the total message count and processing time, are printed for each subgraph.
Example Output

The output will show the number of nodes and edges in each partition, along with the total number of messages exchanged and the processing time.

```bash
Subgraph has 211 nodes
Subgraph has 27 edges
Processing time for subgraph 0: 3.10 seconds
Total messages exchanged: 5424
```

Testing

Two testing functions are available for unit tests in DistributedNode.py:

    simple_test(): A minimal test that creates two nodes and simulates simple message exchanges.
    main(): A test function that sets up a small distributed system with four nodes and runs the GHS algorithm

To run the test:
```bash
python3 DistributedNode.py
```

License
This project is free to use and distribute under the MIT License.
import unittest
from base.GHS import Node, GHS  # Import Node and GHS from the implementation file

class TestGHSAlgorithm(unittest.TestCase):
    def setUp(self):
        # Set up different graph scenarios for testing
        # Graph 1: Simple connected graph
        self.node_a = Node('A')
        self.node_b = Node('B')
        self.node_c = Node('C')
        self.node_d = Node('D')

        self.node_a.add_neighbor(self.node_b, 1)
        self.node_a.add_neighbor(self.node_c, 3)
        self.node_b.add_neighbor(self.node_a, 1)
        self.node_b.add_neighbor(self.node_c, 2)
        self.node_b.add_neighbor(self.node_d, 4)
        self.node_c.add_neighbor(self.node_a, 3)
        self.node_c.add_neighbor(self.node_b, 2)
        self.node_d.add_neighbor(self.node_b, 4)

        self.nodes_simple_graph = [self.node_a, self.node_b, self.node_c, self.node_d]

        # Graph 2: A disconnected graph
        self.node_e = Node('E')
        self.node_f = Node('F')

        self.nodes_disconnected_graph = [self.node_e, self.node_f]

        # Graph 3: A fully connected graph (triangle)
        self.node_g = Node('G')
        self.node_h = Node('H')
        self.node_i = Node('I')

        self.node_g.add_neighbor(self.node_h, 1)
        self.node_g.add_neighbor(self.node_i, 5)
        self.node_h.add_neighbor(self.node_g, 1)
        self.node_h.add_neighbor(self.node_i, 2)
        self.node_i.add_neighbor(self.node_g, 5)
        self.node_i.add_neighbor(self.node_h, 2)

        self.nodes_fully_connected_graph = [self.node_g, self.node_h, self.node_i]

    def test_simple_graph(self):
        """ Test with a simple connected graph """
        ghs = GHS(self.nodes_simple_graph)
        ghs.run()

        # Assert that all nodes belong to the same fragment after running the algorithm
        expected_fragment = self.node_a.fragment_id
        for node in self.nodes_simple_graph:
            self.assertEqual(node.fragment_id, expected_fragment)

    def test_disconnected_graph(self):
        """ Test with a disconnected graph """
        ghs = GHS(self.nodes_disconnected_graph)
        ghs.run()

        # Assert that both nodes are in different fragments since they are disconnected
        self.assertNotEqual(self.node_e.fragment_id, self.node_f.fragment_id)
        self.assertEqual(self.node_e.fragment_id, 'E')
        self.assertEqual(self.node_f.fragment_id, 'F')



    def test_fully_connected_graph(self):
        """ Test with a fully connected triangle graph """
        ghs = GHS(self.nodes_fully_connected_graph)
        ghs.run()

        # Assert that all nodes belong to the same fragment
        expected_fragment = self.node_g.fragment_id
        for node in self.nodes_fully_connected_graph:
            self.assertEqual(node.fragment_id, expected_fragment)

    def test_large_graph(self):
        """ Test with a larger graph to verify performance and correctness """
        # Create a larger graph programmatically
        large_graph_nodes = [Node(f'N{i}') for i in range(10)]
        
        # Add edges to form a chain
        for i in range(len(large_graph_nodes) - 1):
            large_graph_nodes[i].add_neighbor(large_graph_nodes[i + 1], i + 1)
            large_graph_nodes[i + 1].add_neighbor(large_graph_nodes[i], i + 1)

        ghs = GHS(large_graph_nodes)
        ghs.run()

        # Assert that all nodes belong to the same fragment
        expected_fragment = large_graph_nodes[0].fragment_id
        for node in large_graph_nodes:
            self.assertEqual(node.fragment_id, expected_fragment)

if __name__ == '__main__':
    unittest.main()

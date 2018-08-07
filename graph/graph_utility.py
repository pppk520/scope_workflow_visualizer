from graph.networkx_utility import NetworkXUtility

from graph.node import Node
from graph.edge import Edge

class GraphUtility(object):
    def __init__(self, nodes, edges):
        self.nu = NetworkXUtility()

        for node in nodes:
            self.nu.add_node(node, **node.attr)

        for edge in edges:
            self.nu.add_edge(edge.from_, edge.to_, **edge.attr)

    def to_gexf_file(self, dest_filepath):
        self.nu.to_gexf(dest_filepath)

    def to_dot_file(self, dest_filepath):
        self.nu.to_dot(dest_filepath)


if __name__ == '__main__':
    nodes = []
    edges = []

    nodes.append(Node('a'))
    nodes.append(Node('b'))

    edges.append(Edge('a', 'b'))

    GraphUtility(nodes, edges).to_gexf_file('d:/tmp/tt.gexf')
from graph.networkx_utility import NetworkXUtility

from graph.node import Node
from graph.edge import Edge

from graphviz import Source

class GraphUtility(object):
    def __init__(self, nodes, edges):
        self.nu = NetworkXUtility()

        for node in nodes:
            self.nu.add_node(node, **node.attr)

        for edge in edges:
            self.nu.add_edge(edge.from_, edge.to_, **edge.attr)

    def to_gexf_file(self, dest_file):
        return self.nu.to_gexf(dest_file)

    def to_dot_file(self, dest_file):
        return self.nu.to_dot(dest_file)

    def dot_to_graphviz(self, dot_file_path):
        s = Source.from_file(dot_file_path)
        s.render(dot_file_path, view=False)

if __name__ == '__main__':
    nodes = []
    edges = []

    nodes.append(Node('a'))
    nodes.append(Node('b'))

    edges.append(Edge('a', 'b'))

    GraphUtility(nodes, edges).to_gexf_file('d:/tmp/tt.gexf')
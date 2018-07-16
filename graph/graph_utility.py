from graph.networkx_utility import NetworkXUtility

from graph.node import Node
from graph.edge import Edge

class GraphUtility(object):
    def to_gexf_file(self, nodes, edges, dest_filepath):
        nu = NetworkXUtility()

        for node in nodes:
            nu.add_node(node, **node.attr)

        for edge in edges:
            nu.add_edge(edge.from_, edge.to_, **edge.attr)

        nu.to_gexf(dest_filepath)


if __name__ == '__main__':
    nodes = []
    edges = []

    nodes.append(Node('a'))
    nodes.append(Node('b'))

    edges.append(Edge('a', 'b'))

    GraphUtility().to_gexf_file(nodes, edges, 'd:/tmp/tt.gexf')
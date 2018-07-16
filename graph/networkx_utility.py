import networkx as nx

class NetworkXUtility(object):
    def __init__(self):
        self.g = nx.DiGraph()

    def add_node(self, node, **attr):
        self.g.add_node(node, **attr)

    def add_edge(self, from_node, to_node, **attr):
        self.g.add_edge(from_node, to_node, **attr)

    def to_gexf(self, dest_path):
        if not dest_path.endswith('.gexf'):
            dest_path += '.gexf'

        nx.write_gexf(self.g, dest_path)

if __name__ == '__main__':
    nu = NetworkXUtility()
    nu.add_node(1, label='one')
    nu.add_node(2, label='two')
    nu.add_edge(1, 2)

    print(nu.g.nodes)
    print(nu.g.edges)

    nu.to_gexf('d:/tmp/tt.gexf')
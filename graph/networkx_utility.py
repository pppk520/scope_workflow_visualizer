import networkx as nx

try:
    import pygraphviz
    from networkx.drawing.nx_agraph import write_dot
    print("using package pygraphviz")
except ImportError:
    try:
        import pydot
        from networkx.drawing.nx_pydot import write_dot
        print("using package pydot")
    except ImportError:
        print()
        print("Both pygraphviz and pydot were not found ")
        print("see  https://networkx.github.io/documentation/latest/reference/drawing.html")
        print()
        raise

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

        return dest_path

    def to_dot(self, dest_path):
        if not dest_path.endswith('.dot'):
            dest_path += '.dot'

        write_dot(self.g, dest_path)

        return dest_path

if __name__ == '__main__':
    nu = NetworkXUtility()
    nu.add_node(1, label='one')
    nu.add_node(2, label='two')
    nu.add_edge(1, 2)

    print(nu.g.nodes)
    print(nu.g.edges)

    nu.to_gexf('d:/tmp/tt.gexf')
    nu.to_dot('d:/tmp/tt.dot')
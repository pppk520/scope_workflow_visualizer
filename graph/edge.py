class Edge(object):
    def __init__(self, from_, to_, attr={}):
        self.from_ = from_
        self.to_ = to_
        self.attr = attr

    def __str__(self):
        return '{} -> {}'.format(self.from_, self.to_)

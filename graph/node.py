import uuid


class Node(object):
    def __init__(self, name, attr={}):
        self.name = name
        self.attr = attr.copy()

        self.attr['id'] = uuid.uuid4()
        self.attr['label'] = name

    def __str__(self):
        return self.name
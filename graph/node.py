import uuid


class Node(object):
    def __init__(self, name, attr={}):
        self.name = name
        self.attr = attr.copy()

        if not 'id' in attr:
            self.attr['id'] = uuid.uuid4()

        if not 'label' in attr:
            self.attr['label'] = name

    # pydot use this as unique identity
    # if you want to treat nodes with the same name as one node, return name
    # otherwise, return id
    def __str__(self):
        return self.name
import uuid


class Node(object):
    def __init__(self, name, attr={}):
        self.name = name
        self.attr = attr.copy()

        if not 'id' in attr:
            self.attr['id'] = uuid.uuid4()

        if not 'label' in attr:
            self.attr['label'] = name

    def __str__(self):
        return self.name
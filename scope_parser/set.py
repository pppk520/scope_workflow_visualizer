from pyparsing import *
from scope_parser.common import Common

class Set(object):
    SET = Keyword("#SET")

    comment = Common.comment
    ident = Common.ident

    set_ = SET + Combine(ident)('key') + '=' + restOfLine('value')
    set_.ignore(comment)

    def parse(self, s):
        data = self.set_.searchString(s)

        if data:
            return data[0][1], data[0][-1]

        return None, None
if __name__ == '__main__':
    d = Set()

    print(d.parse('#SET OPT_PATH = "/local/prod/pipelines/Optimization/";'))

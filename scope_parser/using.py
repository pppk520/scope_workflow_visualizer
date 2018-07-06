from pyparsing import *
from scope_parser.common import Common

class Using(object):
    USING = Keyword("USING")

    value_str = Common.value_str

    using = USING + Combine(value_str)('path')

    def parse(self, s):
        data = self.using.parseString(s)

        return data['path']

if __name__ == '__main__':
    obj = Using()

    print(obj.parse('USING ImpressionShareLib;'))

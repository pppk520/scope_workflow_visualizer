from pyparsing import *
from scope_parser.common import Common

class Using(object):
    USING = Keyword("USING")

    ident_dot = Common.ident_dot;

    using = USING + Combine(ident_dot)('path')

    def parse(self, s):
        data = self.using.parseString(s)

        return data['path']

if __name__ == '__main__':
    obj = Using()

    print(obj.parse('USING AdInsight.Common.SharedSchema.LogRecord.Bid'))

from pyparsing import *
from scope_parser.common import Common

class Module(object):
    MODULE = Keyword("MODULE")
    AS = Keyword("AS")

    comment = Common.comment
    ident = Common.ident
    value_str = Common.value_str

    module = MODULE + Combine(value_str)('path') + AS + Combine(ident)('name')
    module.ignore(comment)

    def parse(self, s):
        data = self.module.parseString(s)

        return data['name'], data['path']

if __name__ == '__main__':
    obj = Module()

    print(obj.parse('MODULE @MonetizationModulePATH AS MonetizationModules'))

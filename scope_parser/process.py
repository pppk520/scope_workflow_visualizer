from pyparsing import *
from common import Common


class Process(object):
    PROCESS = Keyword("PROCESS")
    USING = Keyword("USING")

    ident = Common.ident
    func = Common.func

    process_stmt = ident + '=' + PROCESS + ident + USING + func


    def parse(self, s):
        return self.process_stmt.parseString(s)

if __name__ == '__main__':
    obj = Process()

    print(obj.parse('IS_ORDER_BSC = PROCESS IS_ORDER_BSC USING StripePartitionLookupProcessor("PipelineConfiguration.xml")'))

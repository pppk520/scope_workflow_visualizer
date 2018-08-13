from pyparsing import *
from scope_parser.common import Common


class Process(object):
    PROCESS = Keyword("PROCESS")
    PRODUCE = Keyword("PRODUCE")
    USING = Keyword("USING")
    HAVING = Keyword("HAVING")

    ident = Common.ident
    produce_schema = delimitedList(ident)
    func = Common.func
    func_ptr = Common.func_ptr

    process_stmt = Combine(ident)('assign_var') + '=' + PROCESS + Combine(ident)('source') + Optional(PRODUCE + produce_schema) + USING + (func | func_ptr)('using')


    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set(),
            'using': None
        }

        d = self.process_stmt.parseString(s)

        ret['assign_var'] = d['assign_var']
        ret['sources'].add(d['source'])
        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Process()

    print(obj.parse('''
Suggestions = 
    PROCESS Suggestions
    USING Utils.RankingProcessor("AdvertiserIntelligenceConfig.txt","[Algo1]");
        '''))




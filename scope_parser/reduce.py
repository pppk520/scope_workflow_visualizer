from pyparsing import *
from scope_parser.common import Common


class Reduce(object):
    REDUCE = Keyword("REDUCE")
    USING = Keyword("USING")
    PRESORT = Keyword("PRESORT")
    ON = Keyword("ON")
    PRODUCE = Keyword("PRODUCE")

    ident = Common.ident
    produce_schema = delimitedList(ident)
    func = Common.func
    func_ptr = Common.func_ptr

    on = Optional(ON + delimitedList(ident))
    presort = Optional(PRESORT + delimitedList(ident + Optional(oneOf('DESC ASC'))))
    produce = Optional(PRODUCE + delimitedList(ident))
    using = Optional(USING + (func | func_ptr)('using'))
    reduce_stmt = Combine(ident)('assign_var') + '=' + REDUCE + Combine(ident)('source') + on + produce + presort + using

    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set(),
            'using': None
        }

        d = self.reduce_stmt.parseString(s)

        ret['assign_var'] = d['assign_var']
        ret['sources'].add(d['source'])
        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Reduce()

    print(obj.parse('''
Suggestions =
    REDUCE Suggestions
    ON Key
    PRESORT AccountId ASC, Score DESC, MonthlyQueryVolume DESC
    USING GroupRankReducer("1", @MaxSuggCount)
        '''))



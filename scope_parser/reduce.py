import json
from pyparsing import *
from scope_parser.common import Common
from scope_parser.select import Select


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

    select_stmt = Select.select_stmt

    on = Optional(ON + delimitedList(ident))
    presort = Optional(PRESORT + delimitedList(ident + Optional(oneOf('DESC ASC'))))
    produce = Optional(PRODUCE + delimitedList(ident))
    using = Optional(USING + (func | func_ptr)('using'))
    reduce_stmt = Combine(ident)('assign_var') + '=' + REDUCE + (Combine(ident)('source') | select_stmt('select_stmt')) + on + produce + presort + using

    def debug(self):
        print(self.using.parseString('USING GroupingReducer("SuggKW", "3")'))
        print(self.presort.parseString('PRESORT Score DESC'))

    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set(),
            'using': None
        }

        d = self.reduce_stmt.parseString(s)

#        print('-' *20)
#        print(json.dumps(d.asDict(), indent=4))
#        print('-' *20)

        ret['assign_var'] = d['assign_var']

        if 'source' in d:
            ret['sources'].add(d['source'])
        else:
            ret['sources'].add(d['table_name']) # directly from SELECT statement

        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Reduce()
    obj.debug()

    print(obj.parse('''
KWOSuggestions_NoneChinese_Others =
    REDUCE
    (
        SELECT AccountId,
               OrderId,
               SuggKW,
               SuggMatchTypeId,
               TrackId,
               TrafficId,
               KeyTerm,
               BMMKeyword,
               OptTypes,
               Score,
               LCID_C2C,
               bChinese
        FROM KWOSuggestions_NoneChinese
        WHERE Theme == "Keyword Like" OR Theme == ""
    )
    ON OrderId
    USING GroupingReducer("SuggKW", "3")
    PRESORT Score DESC
        '''))



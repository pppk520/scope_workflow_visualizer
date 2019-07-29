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

    on = ON + delimitedList(ident)('on')
    presort = PRESORT + delimitedList(ident + Optional(oneOf('DESC ASC')))('presort')
    produce = PRODUCE + delimitedList(ident)('produce')
    using = USING + (func | func_ptr)('using')
    recude_each = Each([on, Optional(produce), Optional(presort), Optional(using)])

    reduce_explicit = REDUCE + (Combine(ident)('source') | select_stmt('select_stmt')) + recude_each
    reduce_implicit = REDUCE + recude_each
    reduce = reduce_explicit | reduce_implicit

    assign_reduce = Combine(ident)('assign_var') + '=' + reduce
    reduce_stmt = assign_reduce | reduce

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

        if 'assign_var' in d:
            ret['assign_var'] = d['assign_var']

        if 'source' in d:
            ret['sources'].add(d['source'])
        elif 'table_name' in d:
            ret['sources'].add(d['table_name'])  # from SELECT statement
        else:
            # implicit reduce
            pass

        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Reduce()
#    obj.debug()

    print(obj.parse('''
REDUCE ON OrderItemId USING PassThroughReducer;
        '''))



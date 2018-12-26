import json
from pyparsing import *
from scope_parser.common import Common
from scope_parser.select import Select


class Combine(object):
    COMBINE = Keyword("COMBINE")
    USING = Keyword("USING")
    ON = Keyword("ON")
    WITH = Keyword("WITH")

    ident = Common.ident
    ident_at = Common.ident_at
    sstream_ident = '(' + 'SSTREAM' + (ident_at | ident) + ')'
    produce_schema = delimitedList(ident)
    func = Common.func
    func_ptr = Common.func_ptr

    select_stmt = Select.select_stmt
    as_something = Select.as_something
    where_expression = Select.where_expression

    combine_source = ident | sstream_ident

    using = Optional(USING + (func | func_ptr)('using'))
    combine_with = COMBINE + \
                   Combine(combine_source)('source_1') + Optional(as_something) + \
                   WITH + \
                   Combine(combine_source)('source_2') + Optional(as_something) + \
                   ON + where_expression + \
                   USING + (func | func_ptr)('using')

    assing_combine = Combine(ident)('assign_var') + '=' + combine_with

    def debug(self):
        print(self.combine_with.parseString('''
            COMBINE Suggestions AS L WITH OrderTermBag AS R ON L.OrderId == R.OrderId USING SuggestionTfCombiner("TFIDF");
        '''))

    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set(),
            'using': None
        }

        d = self.assing_combine.parseString(s)

#        print('-' *20)
#        print(json.dumps(d.asDict(), indent=4))
#        print('-' *20)

        ret['assign_var'] = d['assign_var']
        ret['sources'].add(d['source_1'])
        ret['sources'].add(d['source_2'])
        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Combine()
    obj.debug()

    print(obj.parse('''
OrderBiddingKW =
    COMBINE OrderBiddingKW AS L WITH (SSTREAM@OrderTFScoreFile) AS R
    ON L.OrderId == R.OrderId
    USING SuggestionTfCombiner("TFIDF");

        '''))











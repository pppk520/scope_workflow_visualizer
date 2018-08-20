import json
from pyparsing import *
from scope_parser.common import Common
from scope_parser.select import Select


class Output(object):
    OUTPUT = Keyword("OUTPUT")
    TO = Keyword("TO")
    SSTREAM = Keyword("SSTREAM")
    WITH_STREAMEXPIRY = Keyword("WITH STREAMEXPIRY")
    PARTITIONED_BY = Keyword("PARTITIONED BY")
    USING = Keyword("USING")
    WHERE = Keyword("WHERE")
    CLUSTERED_BY = Keyword("CLUSTERED BY")
    SORTED_BY = Keyword("SORTED BY")

    ident = Common.ident
    value_str = Common.value_str
    func = Common.func

    select_stmt = Select.select_stmt

    with_streamexpiry = Group(WITH_STREAMEXPIRY + value_str)
    partitioned_by = PARTITIONED_BY + ident
    using = USING + func
    clustered_by = Group(Optional(oneOf('HASH')) + CLUSTERED_BY + ident)
    sorted_by = SORTED_BY + ident

    simple_where = Group(WHERE + ident + oneOf("== != >= <= > <") + (oneOf("true false") | value_str))

    output_sstream = OUTPUT + (((ident('ident') | select_stmt) + TO) | TO) + Optional(SSTREAM)('sstream') + value_str('path') + \
                     Optional(clustered_by) + \
                     Optional(sorted_by) + \
                     Optional(partitioned_by)('partition') + \
                     Optional(with_streamexpiry) + \
                     Optional(simple_where) + \
                     Optional(using)

    output = output_sstream

    def parse(self, s):
        # specific output for our purpose
        ret = {
            'ident': None,
            'path': None,
            'stream_type': None,
            'attributes': set()
        }

        data = self.output.parseString(s)

#        print('-' *20)
#        print(json.dumps(data.asDict(), indent=4))
#        print('-' *20)

        if 'ident' in data:
            ret['ident'] = data['ident'][0]
        elif 'table_name' in data:
            ret['ident'] = data['table_name'] # directly from SELECT statement

        ret['path'] = data['path']

        if 'sstream' in data:
            ret['stream_type'] = 'SSTREAM'

        if 'partition' in data:
            ret['attributes'].add('PARTITION')

        return ret

if __name__ == '__main__':
    obj = Output()

    print(obj.parse('''
        OUTPUT (SELECT * FROM Suggestions WHERE bNeedFiltered == true) TO SSTREAM @Output_FilteredBy_SpecialRuleForCustomerFilter CLUSTERED BY OrderId WITH STREAMEXPIRY @STREAM_EXPIRY
    '''))

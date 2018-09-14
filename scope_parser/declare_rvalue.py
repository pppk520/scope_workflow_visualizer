from pyparsing import *
from scope_parser.common import Common
import re
import json
from util.parse_util import ParseUtil

class DeclareRvalue(object):
    comment = Common.comment

    ident = Common.ident
    func_chain = Common.func_chain

    keyword_string_format = Keyword('string.Format') | Keyword('String.Format')

    param = Combine('@' + ident)
    param_str = Combine(Optional('@') + quotedString)
    param_str_cat = Group((param_str | param) + OneOrMore(oneOf('+ - * /') + (func_chain | param_str | param))) # delimitedList suppress delim, we want to keep it
    num_operation = Group(Word(nums) + OneOrMore(oneOf('+ - * /') + Word(nums)))
    format_item = func_chain('func_chain') | Combine(num_operation)('num_operation') | param_str_cat('param_str_cat') | param_str('param_str') | param('param') | Combine(ident('ident'))
    placeholder_basic = Group('{' + Word(nums) + '}')
    placeholder_date = Group('{' + Word(nums) + ':' + delimitedList(oneOf('yyyy MM dd'), delim=oneOf('/ - _')) + '}')
    string_format = keyword_string_format + '(' + Combine(param_str_cat | param_str | param)('format_str') + ZeroOrMore(',' + format_item('format_item*')) + ')'
    boolean_values = oneOf('true false')

    rvalue = string_format('str_format') | param_str_cat('str_cat') | Word(nums + '.')('nums') | func_chain('func_chain') | param_str('param_str') | param('param') | boolean_values('boolean')

    def debug(self):
        data = self.format_item.parseString('"/shares/bingads.algo.prod.adinsights/data/prod/pipelines/ImpressionShare/Common"+"/%Y/%m/%d/DSAMerge%Y%m%d%h.ss?date={0}&hour={1}"')
#        data = self.format_item.parseString('"2018-01-01"')

        print(data)

    def parse(self, s):
        result = self.rvalue.parseString(s)

#        print('-' *20)
#        print(json.dumps(result.asDict(), indent=4))
#        print('-' *20)

        ret = {
            'format_str': None,
            'format_items': None,
            'type': None
        }

        if 'format_str' in result:
            format_str = result['format_str']

            # keep double quotes for later concat strings
            if not ParseUtil.is_extend_str_cat(format_str):
                ret['format_str'] = format_str.lstrip('@').lstrip('"').rstrip('"')
            else:
                ret['format_str'] = format_str.lstrip('@')

            ret['format_items'] = list(result['format_item'])
            ret['type'] = 'format_str'
        elif 'str_cat' in result:
            ret['format_items'] = list(result['str_cat'])
            ret['type'] = 'str_cat'
        elif 'nums' in result:
            # float parsed by default
            the_num = result['nums']
            ret['format_items'] = [the_num,]
            ret['type'] = 'nums'
        elif 'func_chain' in result:
            # dirty trick for str_cat params
            match = re.match('(.*)\("(.*)"\)', result['func_chain'])
            if match:
                func_name = match.group(1)
                params = '"' + match.group(2).replace('"', '') + '"'

                ret['format_items'] = [func_name + '(' + params + ')', ]
            else:
                ret['format_items'] = [result['func_chain'],]

            ret['type'] = 'func_chain'
        elif 'param_str' in result:
            ret['format_items'] = [result['param_str'], ]
            ret['type'] = 'str_cat'
        elif 'param' in result:
            ret['format_items'] = [result['param'], ]
            ret['type'] = 'str_cat'
        elif 'boolean' in result:
            ret['format_items'] = [result['boolean'], ]
            ret['type'] = 'boolean'

        return ret

if __name__ == '__main__':
    r = DeclareRvalue()
    r.debug()

    print(r.parse('String.Format("{0}/%Y/%m/%d/AuctionJoinPV_%h.ss?date={1}&hour={2}", "/shares/bingads.algo.prod.adinsights/data/shared_data/AdvertiserEngagement/Metallica/prod/ImpressionShare", @DATE_UTC, @hour)'))

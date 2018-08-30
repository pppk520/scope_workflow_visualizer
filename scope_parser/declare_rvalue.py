from pyparsing import *
from scope_parser.common import Common
import re

class DeclareRvalue(object):
    comment = Common.comment

    ident = Common.ident
    func_chain = Common.func_chain

    keyword_string_format = Keyword('string.Format') | Keyword('String.Format')

    param = Combine('@' + ident)
    param_str = Combine(Optional('@') + quotedString)
    param_str_cat = Group((param_str | param) + OneOrMore(oneOf('+ - * /') + (func_chain | param_str | param))) # delimitedList suppress delim, we want to keep it
    format_item = func_chain('func_chain') | param_str('param_str') | param('param') | ident('ident')
    placeholder_basic = Group('{' + Word(nums) + '}')
    placeholder_date = Group('{' + Word(nums) + ':' + delimitedList(oneOf('yyyy MM dd'), delim=oneOf('/ - _')) + '}')
    string_format = keyword_string_format + '(' + Optional('@') + quotedString('format_str') + ZeroOrMore(',' + format_item('format_item*')) + ')'

    rvalue = string_format('str_format') | param_str_cat('str_cat') | Word(nums)('nums') | func_chain('func_chain') | param_str('param_str') | param('param')

    def debug(self):
        data = quotedString.parseString("'aaa'")
        print(data)

    def parse(self, s):
        result = self.rvalue.parseString(s)

        ret = {
            'format_str': None,
            'format_items': None,
            'type': None
        }

        if 'format_str' in result:
            ret['format_str'] = result['format_str'].lstrip('"').rstrip('"')
            ret['format_items'] = result['format_item']
            ret['type'] = 'format_str'
        elif 'str_cat' in result:
            ret['format_items'] = list(result['str_cat'])
            ret['type'] = 'str_cat'
        elif 'nums' in result:
            ret['format_items'] = [result['nums'],]
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


#        print(ret)

        return ret

if __name__ == '__main__':
    r = DeclareRvalue()
    r.debug()

    print(r.parse('@KWRawPath'))

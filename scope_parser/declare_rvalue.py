from pyparsing import *
from scope_parser.common import Common
import re
import json
from util.parse_util import ParseUtil

class DeclareRvalue(object):
    comment = Common.comment

    ident = Common.ident
    class_attr = ident + OneOrMore('.' + ident)
    func_chain = Common.func_chain

    keyword_string_format = Keyword('string.Format') | Keyword('String.Format')

    param = Combine('@' + ident)
    param_str = Combine(Optional('@') + quotedString)
    param_str_cat = Group((param_str | param) + OneOrMore('+' + (func_chain | param_str | param))) # delimitedList suppress delim, we want to keep it
    num_operation = Group(Word(nums) + OneOrMore(oneOf('+ - * /') + Word(nums)))
    format_item = func_chain('func_chain') | Combine(num_operation)('num_operation') | param_str_cat('param_str_cat') | param_str('param_str') | param('param') | Combine(ident('ident'))
    placeholder_basic = Group('{' + Word(nums) + '}')
    placeholder_date = Group('{' + Word(nums) + ':' + delimitedList(oneOf('yyyy MM dd'), delim=oneOf('/ - _')) + '}')
    format_str = Combine(param_str_cat | param_str | param, adjacent=False)
    string_format = keyword_string_format + '(' + format_str('format_str') + ZeroOrMore(',' + format_item('format_item*')) + ')'
    boolean_values = oneOf('true false')
    if_assignment = Regex('IF.*\((.*),(.*),(.*)\)')

    rvalue = string_format('str_format') | \
             param_str_cat('str_cat') | \
             Word(nums + '.')('nums') | \
             func_chain('func_chain') | \
             param_str('param_str') | \
             param('param') | \
             boolean_values('boolean') | \
             Combine(class_attr)('class_attr') | \
             if_assignment('if_assignment')

    def debug(self):
        result = self.format_str.parseString('"/local/prod/pipelines/Opportunities/output/BudgetEnhancement/AIMTBondResult//2018/09/21" + "/BudgetOpt_Bond_PKV_Table_{0:yyyyMMdd}.ss"')

        print('-' *20)
        print(json.dumps(result.asDict(), indent=4))
        print('-' *20)

    def parse(self, s):
        result = self.rvalue.parseString(s)

#        print('-' *20)
#        print(json.dumps(result.asDict(), indent=4))
#        print('-' *20)

        ret = {
            'format_str': None,
            'format_items': [],
            'type': None
        }

        if 'format_str' in result:
            format_str = result['format_str']

            # keep double quotes for later concat strings
            if not ParseUtil.is_extend_str_cat(format_str):
                ret['format_str'] = format_str.lstrip('@').lstrip('"').rstrip('"')
            else:
                ret['format_str'] = format_str.lstrip('@')

            ret['type'] = 'format_str'

            if 'format_item' in result:
                ret['format_items'] = list(result['format_item'])
        elif 'str_cat' in result:
            ret['format_items'] = list(result['str_cat'])
            ret['type'] = 'str_cat'
        elif 'nums' in result:
            # float parsed by default
            the_num = result['nums']
            ret['format_items'] = [the_num,]
            ret['type'] = 'nums'
        elif 'func_chain' in result:
            ret['format_items'] = [result['func_chain'], ]
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
        elif 'class_attr' in result:
            ret['format_items'] = [result['class_attr'], ]
            ret['type'] = 'class_attr'
        elif 'if_assignment' in result:
            ret['format_items'] = ["", ]
            ret['type'] = 'if_assignment'

        return ret

if __name__ == '__main__':
    r = DeclareRvalue()
    r.debug()

    print(r.parse('@OutputFolder.Trim()+@"/CookedBulkApiLogs/FilteredBulkApiLog_"+@TodayStr+".ss"'))
    print(r.parse('@"/CookedBulkApiLogs/FilteredBulkApiLog_" + @OutputFolder.Trim() + @TodayStr+".ss"'))

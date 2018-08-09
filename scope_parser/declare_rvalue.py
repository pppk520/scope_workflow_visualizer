from pyparsing import *
from scope_parser.common import Common

class DeclareRvalue(object):
    comment = Common.comment

    ident = Common.ident
    func_chain = Common.func_chain

    param = Combine('@' + ident)
    param_str = Combine(Optional('@') + quotedString)
    param_str_cat = Group((param_str | param) + ZeroOrMore(oneOf('+ - * /') + (param_str | param))) # delimitedList suppress delim, we want to keep it
    format_item = func_chain('func_chain') | param_str('param_str') | param('param') | ident('ident')
    placeholder_basic = Group('{' + Word(nums) + '}')
    placeholder_date = Group('{' + Word(nums) + ':' + delimitedList(oneOf('yyyy MM dd'), delim=oneOf('/ - _')) + '}')
    string_format = 'string.Format(' + Optional('@') + quotedString('format_str') + ZeroOrMore(',' + format_item('format_item*')) + ')'

    rvalue = string_format('str_format') | param_str_cat('str_cat') | Word(nums)('nums') | func_chain('func_chain')

    def debug(self):
        data = self.param_str_cat.parseString('"abc" + "111"')
        print(data)

    def parse(self, s):
        result = self.rvalue.parseString(s)

        ret = {
            'format_str': None,
            'format_items': None,
            'type': None
        }

        if 'format_str' in result:
            ret['format_str'] = result['format_str']
            ret['format_items'] = result['format_item']
            ret['type'] = 'format_str'
        elif 'str_cat' in result:
            ret['format_items'] = list(result['str_cat'])
            ret['type'] = 'str_cat'
        elif 'nums' in result:
            ret['format_items'] = [result['nums'],]
            ret['type'] = 'nums'
        elif 'func_chain' in result:
            ret['format_items'] = [result['func_chain'],]
            ret['type'] = 'func_chain'

#        print(ret)

        return ret

if __name__ == '__main__':
    r = DeclareRvalue()
    r.debug()

    print(r.parse('DateTime.Parse("@@RunDate@@")'))
    print(r.parse('@"@@KWRawPath@@"'))
    print(r.parse('@"@@KWRawPath@@"; //comment'))
    print(r.parse('int.Parse("@@AuctionDataGetRatio@@");'))
    print(r.parse('66'))
    print(r.parse('"abc" + "111"'))
    print(r.parse('"@RunDate"'))
    print(r.parse('@DebugFolder + "AuctionWithUpdatedPclick.ss"'))
    print(r.parse('string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));'))
    print(r.parse('string.Format("{0}/BidEstimation/Result/%Y/%m/KeywordAuction_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", @BTEPath, @BTERunDate);'))
    print(r.parse('string.Format("{0}/{1:yyyy/MM/dd}/Delta{2}_", @"/local/prod/pipelines/Optimization/KeywordOpportunity/Preparations/MPIProcessing/Debug", DateTime.Parse(@RunDate), @DateDelta); '))

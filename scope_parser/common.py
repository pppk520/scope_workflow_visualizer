from pyparsing import *


class Common(object):
    comment = "//" + restOfLine
    ident = Group(Word('_<>[]*' + alphanums)).setName("identifier")
    ident_dot = delimitedList(ident, delim='.', combine=True)
    ident_float_suffix = '.' + Word(nums) + Optional('F')
    ident_val = Combine(Word('- ' + nums) + Optional(ident_float_suffix | 'UL'))
    value_str = Combine(Group(Optional(oneOf('@@ @')) + (ident_val | quotedString | ident) + Optional('@@')))

    quoted_time = Combine('"' + Word(":" + nums) + '"')
    ext_quoted_string = quoted_time | quotedString
    param_str_cat = delimitedList(ext_quoted_string, delim='+')

    nullible = Group('(' + ident + '??' + ident + ')')
    expr_item_general = Word(printables + ' ', excludeChars=':(),+-*/|') | nullible
    expr_item_parentheses = '(' + expr_item_general + ')'
    expr_item = expr_item_parentheses | expr_item_general
    expr = expr_item + ZeroOrMore(oneOf('+ - * / |') + expr_item)
    func = Forward()
    func_ptr = Forward()
    func_params = delimitedList(param_str_cat | expr | ident | Word('-' + nums))

    param_lambda = Group(Optional('(') + delimitedList(ident) + Optional(')') + '=>' + OneOrMore(func | ident))
    func_lambda = Group(delimitedList(ident, delim='.', combine=True) + Group('(' + param_lambda + ')'))

    func <<= func_lambda | Group(delimitedList(ident, delim='.', combine=True) + Group('(' + Optional(func | func_params) + ')'))
    func_ptr <<= Group(delimitedList(ident, delim='.', combine=True))

    func_chain = Combine(Optional('@') + delimitedList(func, delim='.', combine=True))

if __name__ == '__main__':
    obj = Common()

    print(obj.expr.parseString('(CountryCode[0]<< 8) | CountryCode[1]'))
    print(obj.param_lambda.parseString('a => new BidHistory(a)'))
    print(obj.func_lambda.parseString('Select(a => new BidHistory(a))'))
    print(obj.func.parseString('Select(a => new BidHistory(a))'))
    print(obj.func_chain.parseString("History.Split(';').Select(a => new BidHistory(a)).ToList()"))
    print(obj.value_str.parseString("6"))


    '''
    print(obj.func_params.parseString('MinBid * (ExchangeRateUSD ?? 1m) * 100 - 0.01m'))
    print(obj.func.parseString("Math.Ceiling(MinBid * (ExchangeRateUSD ?? 1m) * 100 - 0.01m)"))
    print(obj.ident.parseString('B.SpendUSD??0'))
    print(obj.value_str.parseString("- 1"))
    print(obj.param_str_cat.parseString('"2018" + " " + ":00:00" + "20"'))
    print(obj.func_params.parseString('"2018" + " " + ":00:00" + "20"'))
    print(obj.func.parseString('DateTime.Parse("2018" + " " + "20" + ":00:00")'))
    print(obj.func.parseString("Convert.ToUInt32(SuggBid * 100)"))
    print(obj.func.parseString("Convert.ToUInt32(1, 2, 3)"))
    out = obj.func.parseString('DateTime.Parse("2018" + " " + "20" + ":00:00")')
    print(out.asDict())

    print(obj.func.parseString("FIRST(YouImpressionCnt)"))
    print(obj.func.parseString('ToString("yyyy-MM-dd")'))
    print(obj.expr.parseString("SuggBid * 100"))
    print(obj.func.parseString("COUNT(DISTINCT (OrderId))"))
    print(obj.ident.parseString("100"))
    print(obj.value_str.parseString("1.0"))
    print(obj.value_str.parseString("1.0F"))
    print(obj.ident_val.parseString("1.0F"))
    print(obj.value_str.parseString("-1"))
    print(obj.value_str.parseString("0UL"))
    '''
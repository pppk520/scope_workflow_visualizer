from pyparsing import *


class Common(object):
    comment = "//" + restOfLine
    ident = Group(Word('_<>' + alphanums)).setName("identifier")
    ident_float_suffix = '.' + Word(nums) + Optional('F')
    ident_val = Combine(Word('- ' + nums) + Optional(ident_float_suffix | 'UL'))
    value_str = Combine(Group(Optional(oneOf('@@ @')) + (ident_val | quotedString | ident) + Optional('@@')))

    quoted_time = Combine('"' + Word(":" + nums) + '"')
    ext_quoted_string = quoted_time | quotedString
    str_cat = delimitedList(ext_quoted_string, delim='+')

    expr = Word(printables + ' ', excludeChars=':(),')
    func = Forward()
    func_ptr = Forward()
    func_params = delimitedList(str_cat | expr | ident | Word('-' + nums))

    func <<= Group(delimitedList(ident, delim='.', combine=True) + Group('(' + Optional(func | func_params) + ')'))
    func_ptr <<= Group(delimitedList(ident, delim='.', combine=True))

    func_chain = Combine(Optional('@') + delimitedList(func, delim='.', combine=True))

if __name__ == '__main__':
    obj = Common()

    print(obj.quoted_time.parseString('":00:00"'))
    print(obj.str_cat.parseString('"2018" + " " + ":00:00" + "20"'))
    print(obj.func_params.parseString('"2018" + " " + ":00:00" + "20"'))
    print(obj.func.parseString('DateTime.Parse("2018" + " " + "20" + ":00:00")'))
    '''
    print(obj.func.parseString("FIRST(YouImpressionCnt)"))
    print(obj.func.parseString('ToString("yyyy-MM-dd")'))
    print(obj.expr.parseString("SuggBid * 100"))
    print(obj.func.parseString("Convert.ToUInt32(SuggBid * 100)"))
    print(obj.func.parseString("COUNT(DISTINCT (OrderId))"))
    print(obj.ident.parseString("100"))
    print(obj.value_str.parseString("1.0"))
    print(obj.value_str.parseString("1.0F"))
    print(obj.ident_val.parseString("1.0F"))
    print(obj.value_str.parseString("-1"))
    print(obj.value_str.parseString("- 1"))
    print(obj.value_str.parseString("0UL"))
    '''
from pyparsing import *


class Common(object):
    comment = "//" + restOfLine
    ident = Group(Word('_<>' + alphanums)).setName("identifier")
    ident_float_suffix = '.' + Word(nums) + Optional('F')
    ident_val = Combine(Word('- ' + nums) + Optional(ident_float_suffix | 'UL'))
    value_str = Combine(Group(Optional(oneOf('@@ @')) + (ident_val | quotedString | ident) + Optional('@@')))

    expr = Word(printables + ' ', excludeChars=':(),')
    func = Forward()
    func_ptr = Forward()
    func_params = delimitedList(expr | ident | Word('-' + nums) | quotedString)

    func <<= Group(delimitedList(ident, delim='.', combine=True) + Combine('(' + Optional(func | func_params) + ')'))
    func_ptr <<= Group(delimitedList(ident, delim='.', combine=True))

    func_chain = delimitedList(func, delim='.', combine=True)

if __name__ == '__main__':
    obj = Common()

    print(obj.func.parseString("FIRST(YouImpressionCnt)"))
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

from pyparsing import *


class Common(object):
    comment = "//" + restOfLine
    ident = Group(Word('_' + alphanums)).setName("identifier")
    value_str = Combine(Group(Optional(oneOf('@@ @')) + (quotedString | ident) + Optional('@@')))

    func_params = delimitedList(ident | Word('-' + nums) | quotedString)
    func = Group(delimitedList(ident, delim='.', combine=True) + Combine('(' + Optional(func_params) + ')'))
    func_chain = delimitedList(func, delim='.', combine=True)


if __name__ == '__main__':
    obj = Common()

    print(obj.func.parseString("FIRST(YouImpressionCnt)"))
from pyparsing import *


class Common(object):
    comment = "//" + restOfLine
    ident = Group(Word('_' + alphanums)).setName("identifier")
    value_str = Combine(Group(Optional('@') + (quotedString | ident)))

    func_params = delimitedList(ident | Word('-' + nums) | quotedString)
    func = Group(delimitedList(ident, delim='.') + Combine('(' + Optional(func_params) + ')'))
    func_chain = delimitedList(func, delim='.', combine=True)
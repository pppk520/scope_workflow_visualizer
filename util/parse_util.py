from pyparsing import *


class ParseUtil(object):
    @staticmethod
    def is_extend_str_cat(the_str):
        ''' Check if it's string concat with numbers inside
        the first item must be string

        :param the_str: the string
        :return: True/False
        '''

        item = quotedString | Word(nums)
        str_cat = quotedString + OneOrMore(Literal('+') + item)

        try:
            str_cat.parseString(the_str)
            return True
        except:
            return False


if __name__ == '__main__':
    print(ParseUtil.is_extend_str_cat('"aa" + "bb"'))
    print(ParseUtil.is_extend_str_cat('"aa" + "bb" + 23'))
    print(ParseUtil.is_extend_str_cat('22 + "bb" + 23'))
    print(ParseUtil.is_extend_str_cat('"22"'))

import re
from pyparsing import *
from scope_parser.common import Common

class Loop(object):
    """ Note: LOOP is officially unrecommended operation

    https://stackoverflow.microsoft.com/questions/5174/where-can-i-find-more-information-about-the-scope-keyword-loop/5175#5175
    """

    LOOP = Keyword("LOOP")
    ident = Common.ident

    loop = LOOP + '(' + Combine(ident)('var') + ',' + Combine(Word(alphanums + '@'))('loop_count') + ')'

    def get_var_loop_count(self, s):
        results = self.loop.searchString(s)

        if len(results) > 0:
            result = results[0]
            return result['var'], result['loop_count']

        return None, None

if __name__ == '__main__':
    lp = Loop()

    print(lp.get_var_loop_count('''
        LOOP(b, 3)
    '''))

    print(lp.get_var_loop_count('''
        LOOP(I, @Var)
    '''))


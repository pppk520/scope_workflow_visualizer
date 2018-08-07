from pyparsing import *
from scope_parser.common import Common


class Reduce(object):
    REDUCE = Keyword("REDUCE")
    USING = Keyword("USING")

    ident = Common.ident
    produce_schema = delimitedList(ident)
    func = Common.func
    func_ptr = Common.func_ptr

    reduce_stmt = Combine(ident)('assign_var') + '=' + REDUCE + Combine(ident)('source') + 'ON' + delimitedList(ident) + USING + (func | func_ptr)

    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set()
        }

        d = self.reduce_stmt.parseString(s)

        ret['assign_var'] = d['assign_var']
        ret['sources'].add(d['source'])

        return ret

if __name__ == '__main__':
    obj = Reduce()

    print(obj.parse('''
PkvBond =
    REDUCE FullSuggestions
    ON AccountId, CampaignId, OrderId
    USING KWOptPkvTableProposalReducer("Opportunity", "Opportunity", "Opportunity");

        '''))

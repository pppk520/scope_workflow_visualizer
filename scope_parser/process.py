from pyparsing import *
from scope_parser.common import Common


class Process(object):
    PROCESS = Keyword("PROCESS")
    PRODUCE = Keyword("PRODUCE")
    USING = Keyword("USING")
    HAVING = Keyword("HAVING")

    ident = Common.ident
    produce_schema = delimitedList(ident)
    func = Common.func

    process_stmt = Combine(ident)('assign_var') + '=' + PROCESS + Combine(ident)('source') + Optional(PRODUCE + produce_schema) + USING + func


    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set()
        }

        d = self.process_stmt.parseString(s)

        ret['assign_var'] = d['assign_var']
        ret['sources'].add(d['source'])

        return ret

if __name__ == '__main__':
    obj = Process()

    print(obj.parse('IS_ORDER_BSC = PROCESS IS_ORDER_BSC USING StripePartitionLookupProcessor("PipelineConfiguration.xml")'))
    print(obj.parse('''
        a = PROCESS KWCandidatesPrepared PRODUCE AccountId, CampaignId, OrderId, OptType, SuggKW, KeyTerm, RGUID, ListingId, CPC, PClick
            USING BTEAdjustmentProcessor()
            HAVING RGUID != ""
        '''))


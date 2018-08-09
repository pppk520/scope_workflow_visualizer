from pyparsing import *
from scope_parser.common import Common

class Declare(object):
    DECLARE = Keyword("#DECLARE")
    DATA_TYPE = oneOf("string DateTime int bool")('data_type')

    ident = Common.ident

    declare = DECLARE + Combine(ident)('key') + DATA_TYPE + '=' + restOfLine('value')

    def parse(self, s):
        data = self.declare.parseString(s)

        if data:
            return data['key'].strip(), data['value'].strip().rstrip(';')

        return None, None

    def debug(self):
        s = 'string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate))'
        print(s[121 - 5: 121 + 5])
        print(self.conversion_stmt.parseString(s))

if __name__ == '__main__':
    d = Declare()
    #d.debug()

    print(d.parse('#DECLARE RunDate DateTime = DateTime.Parse("@@RunDate@@")'))
    print(d.parse('#DECLARE KWRawPath string = @"@@KWRawPath@@"'))
    print(d.parse('#DECLARE KWRawPath string = @"@@KWRawPath@@"; //comment'))
    print(d.parse('#DECLARE AuctionDataGetRatio int = int.Parse("@@AuctionDataGetRatio@@");'))
    print(d.parse('#DECLARE AuctionDataGetRatio int = 66'))
    print(d.parse('#DECLARE CampaignTargetInfo string = string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));'))
    print(d.parse('#DECLARE InputKeywordAuction string = string.Format("{0}/BidEstimation/Result/%Y/%m/KeywordAuction_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", @BTEPath, @BTERunDate);'))
    print(d.parse('#DECLARE DebugFolder string = string.Format("{0}/{1:yyyy/MM/dd}/Delta{2}_", @"/local/prod/pipelines/Optimization/KeywordOpportunity/Preparations/MPIProcessing/Debug", DateTime.Parse(@RunDate), @DateDelta); '))

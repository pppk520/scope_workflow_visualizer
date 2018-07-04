from pyparsing import *
from common import Common

class Declare(object):
    DECLARE = Keyword("#DECLARE")
    DATA_TYPE = oneOf("string DateTime int bool")('data_type')

    comment = Common.comment
    ident = Common.ident

    value_str = Combine(Group(Optional('@') + (quotedString | ident)))
    conversion_prefix = oneOf("int.Parse string.Format DateTime.Parse")

    conversion_stmt = Forward()

    value = conversion_stmt | value_str | Word(nums)
    conversion_stmt << conversion_prefix + '(' + delimitedList(value) + ')'

    declare = DECLARE + ident + DATA_TYPE + '=' + Group(value | conversion_stmt)
    declare.ignore(comment)

    def parse(self, s):
        return self.declare.parseString(s)

    def debug(self):
        s = 'string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate))'
        print(s[121 - 5: 121 + 5])
        print(self.conversion_stmt.parseString(s))

if __name__ == '__main__':
    d = Declare()
    #d.debug()

    print(d.parse('#DECLARE KWRawPath string = @"@@KWRawPath@@"'))
    print(d.parse('#DECLARE KWRawPath string = @"@@KWRawPath@@"; //comment'))
    print(d.parse('#DECLARE AuctionDataGetRatio int = int.Parse("@@AuctionDataGetRatio@@");'))
    print(d.parse('#DECLARE AuctionDataGetRatio int = 66'))
    print(d.parse('#DECLARE CampaignTargetInfo string = string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));'))
    print(d.parse('#DECLARE InputKeywordAuction string = string.Format("{0}/BidEstimation/Result/%Y/%m/KeywordAuction_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", @BTEPath, @BTERunDate);'))
    print(d.parse('#DECLARE DebugFolder string = string.Format("{0}/{1:yyyy/MM/dd}/Delta{2}_", @"/local/prod/pipelines/Optimization/KeywordOpportunity/Preparations/MPIProcessing/Debug", DateTime.Parse(@RunDate), @DateDelta); '))

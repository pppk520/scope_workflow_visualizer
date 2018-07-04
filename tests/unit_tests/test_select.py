from unittest import TestCase
from scope_parser.select import Select

class TestSelect(TestCase):
    def test_parse_basic(self):
        s = '''
            ImpressionShare_Campaign =
            SELECT DateKey,
                   HourNum,
                   AccountId,
                   CampaignId,
                   OrderId,
                   YouOrderItemId,
        
                   MatchTypeId,
                   RelationshipId,
                   DistributionChannelId,
                   MediumId,
                   DeviceTypeId,
        
                   CompAccountId, //CompCampaignId,
                   ImpressionCnt AS ImpressionCntInAuction,
                   0L AS CoImpressionCnt,
                   0L AS PositionNum,
                   0L AS AboveCnt,
                   0L AS TopCnt
            FROM PairAggCampaignAgg;
        '''

        result = Select().parse_assign_select(s)
        print(result)

        self.assertTrue(len(result) > 0)

    def test_parse_union(self):
        s = '''
            a = SELECT *
            FROM Step1
            UNION ALL
            SELECT *
            FROM ImpressionShare
        '''

        result = Select().parse_assign_select(s)
        print(result)

        self.assertTrue(len(result) > 0)

    def test_parse_inner_select(self):
        s = '''
            Merge =
            SELECT DateKey,
                   HourNum,
                   AccountId,
                   CampaignId,
                   OrderId,
                   YouOrderItemId,
        
                   MatchTypeId,
                   RelationshipId,
                   DistributionChannelId,
                   MediumId,
                   DeviceTypeId,
        
                   Domain,
                   SUM(ImpressionCntInAuction) AS ImpressionCntInAuction,
                   //SUM(CoImpression_AuctionLog) AS CoImpression_AuctionLog,    
                   SUM(CoImpressionCnt) AS CoImpressionCnt,
                   SUM(PositionNum) AS PositionNum,
                   SUM(AboveCnt) AS AboveCnt,
                   SUM(TopCnt) AS TopCnt
            FROM
            (
            SELECT *
            FROM Step1
            UNION ALL
            SELECT *
            FROM ImpressionShare
            )
        '''

        result = Select().parse_assign_select(s)
        print(result)

        self.assertTrue(len(result) > 0)


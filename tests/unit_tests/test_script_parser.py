from unittest import TestCase

from myparser.script_parser import ScriptParser


class TestScriptParser(TestCase):
    s_declare = '''
    #DECLARE foo string = "test";
    '''

    s_select_no_where = '''
        EligibleRGuids = 
        SELECT RGUID,   
            ListingId AS OrderItemId,
            MatchTypeId,
            RelationshipId,
            DistributionChannelId,
            MediumId,
            DeviceTypeId,
            FraudQualityBand,
            NetworkId,
            DateKey,
            HourNum
        FROM (SSTREAM @EligibleRGuids);
     '''

    s_select_where = '''
        ClickRows_AdvertiserClicks = 
            SELECT
                RGUID,
                (long)IF(OrderItemId==null,0,OrderItemId) AS OrderItemId,
                COUNT() AS AdvertiserClicks
            FROM
                Monetization_Clicks
            WHERE 
                IsFraud == false
            ; 
     '''

    s_select_union = '''
        RowsWithTotalClicksAttached =
            SELECT
                L.*,
                R.AdvertiserClicks AS AdvertiserClicks,
                R.TotalClicks AS TotalClicks
            FROM
                EligibleRGuids AS L
                INNER JOIN
                Clicks AS R
                ON
                    L.RGUID == R.RGUID && L.OrderItemId == R.OrderItemId
                ;    
    '''


    def setUp(self):
        self.sp = ScriptParser()

    def test_parse_select_no_where(self):
        groups = self.sp.parse_select(self.s_select_no_where)
        print(groups)

        self.assertTrue(len(groups) == 2)

    def test_parse_select_where(self):
        groups = self.sp.parse_select(self.s_select_where)
        print(groups)

        self.assertTrue(len(groups) == 3)

    def test_parse_select_union(self):
        groups = self.sp.parse_select(self.s_select_where)
        print(groups)

        self.assertTrue(len(groups) == 3)



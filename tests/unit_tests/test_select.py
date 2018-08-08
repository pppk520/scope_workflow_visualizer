from unittest import TestCase
from scope_parser.select import Select

class TestSelect(TestCase):
    def test_parse_basic(self):
        s = '''
            ImpressionShare_Campaign =
            SELECT DateKey,
                   HourNum,
                   0L AS AboveCnt,
                   0L AS TopCnt
            FROM PairAggCampaignAgg;
        '''

        result = Select().parse_assign_select(s)

        self.assertTrue(len(result) > 0)
        self.assertTrue(result['assign_var'] == 'ImpressionShare_Campaign')
        self.assertTrue(result['from'][1] == 'PairAggCampaignAgg')

    def test_parse_union(self):
        s = '''
            a = SELECT *
            FROM Step1
            UNION ALL
            SELECT *
            FROM ImpressionShare
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'a')
        self.assertTrue(len(result['sources']) == 2)

    def test_parse_inner_select(self):
        s = '''
            Merge =
            SELECT DateKey,
                   Domain,
                   SUM(ImpressionCntInAuction) AS ImpressionCntInAuction,
                   //SUM(CoImpression_AuctionLog) AS CoImpression_AuctionLog,    
                   SUM(CoImpressionCnt) AS CoImpressionCnt,
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

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'Merge')
        self.assertCountEqual(result['sources'], ['Step1', 'ImpressionShare'])

    def test_parse_inner_join_same_table(self):
        s = '''
            SELECT L.DateKey,
                   L.HourNum,
                   L.RGUID,
                   R.PositionNum,
                   IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
                   R.TopCnt
            FROM AdImpressionRaw AS L
                 INNER JOIN
                     AdImpressionRaw AS R
                 ON L.RGUID == R.RGUID
            WHERE L.OrderItemId != R.OrderItemId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] is None)
        self.assertCountEqual(result['sources'], ['AdImpressionRaw'])

    def test_parse_inner_join_two_table(self):
        s = '''
            data = SELECT L.DateKey,
                   R.PositionNum,
                   IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
                   R.TopCnt
            FROM LeftTable AS L
                 INNER JOIN
                     RightTable AS R
                 ON L.RGUID == R.RGUID
            WHERE L.OrderItemId != R.OrderItemId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'data')
        self.assertCountEqual(result['sources'], ['LeftTable', 'RightTable'])

    def test_parse_if_stmt(self):
        s = '''
            SELECT L.DateKey,
                   IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
                   R.TopCnt
            FROM Table1, Table2
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] is None)
        self.assertCountEqual(result['sources'], ['Table1', 'Table2'])

    def test_parse_from_input_module(self):
        s = '''
            SELECT L.DateKey,
                   IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
                   R.TopCnt
            FROM(
                MonetizationModules.MonetizationImpression(
                    INPUT_BASE = @MonetizationCommonDataPath, 
                    START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
                    END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
                )        
            )
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] is None)
        self.assertCountEqual(result['sources'], ['MODULE_MonetizationModules.MonetizationImpression'])

    def test_parse_from_input_module_complex(self):
        s = '''
            AdImpressionRaw = SELECT 
                    CampaignTZDateKey AS DateKey,
                    AdId??0UL AS AdId,
                    (byte?)MatchTypeId AS MatchTypeId,
                    AbsPosition AS PositionNum,
                    PagePosition.StartsWith("ML")?1:0 AS TopCnt,
                    ImpressionCnt
            FROM (
                MonetizationModules.MonetizationImpression(
                    INPUT_BASE = @@MonetizationCommonDataPath@@, 
                    START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
                    END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
                ))
            WHERE IsFraud == false && DupAdId == 0 && AdDisplayTypeId != 5 && MediumId IN (1,3) LogDelta == @DateObj
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AdImpressionRaw')
        self.assertCountEqual(result['sources'], ['MODULE_MonetizationModules.MonetizationImpression'])

    def test_parse_from_nowhere(self):
        s = '''
            RguidLevelAgg =
                SELECT DateKey,
                       HourNum,
                       GeoLocationId,
                       FIRST(YouImpressionCnt) AS YouImpressionCnt,
                       RGUID,
                       CompAccountId,
                       AdId,
                       SUM(AboveCnt) AS AboveCnt,
                       SUM(TopCnt) AS TopCnt    
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'RguidLevelAgg')
        self.assertCountEqual(result['sources'], [])

    def test_parse_from_input_sstream(self):
        s = '''
            AdDispayUrl =
                SELECT ulong.Parse(AdId) AS AdId,
                       long.Parse(ListingId) AS ListingId,
                       Domain
                FROM
                (
                    SSTREAM @FinalDomainPath
                )
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AdDispayUrl')
        self.assertCountEqual(result['sources'], ['SSTREAM_@FinalDomainPath'])

    def test_parse_join_on(self):
        s = '''
            Listign2DomainAgg =
                SELECT DateKey,
                       Domain,
                       SUM(CoImpressionCnt) AS CoImpressionCnt,
                       SUM(TopCnt) AS TopCnt
                FROM Listing2Ad
                     INNER JOIN
                         AdDispayUrl
                     ON Listing2Ad.CompOrderItemId == AdDispayUrl.ListingId && Listing2Ad.AdId == AdDispayUrl.AdId
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'Listign2DomainAgg')
        self.assertCountEqual(result['sources'], ['Listing2Ad', 'AdDispayUrl'])

    def test_parse_no_assign_no_from(self):
        s = '''
            SELECT DateKey,
                   Domain,
                   SUM(CoImpressionCnt) AS CoImpressionCnt
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] is None)
        self.assertCountEqual(result['sources'], [])

    def test_parse_column_func(self):
        s = '''
            KWCandidates =
                SELECT 
                       AccountId,
                       (long)CampaignId AS CampaignId,
                       Convert.ToUInt32(SuggBid * 100) AS SuggBid,
                       OptType
                FROM
                (
                    SSTREAM @Input_Suggestions
                )
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidates')
        self.assertCountEqual(result['sources'], ['SSTREAM_@Input_Suggestions'])

    def test_parse_column_func_logical_not(self):
        s = '''
            KWCandidatesWithLocationTarget =
                SELECT KWCandidatesWithAuctionPassNegKW.AccountId,
                       CampaignTargetInfo.StateIdList,
                       CampaignTargetInfo.MetroAreaIdList,
                       CampaignTargetInfo.CityIdList,
                       CampaignTargetInfo.IsLocationBidAdjustmentEnabled,
                       !(string.IsNullOrEmpty(CountryIdList) AND string.IsNullOrEmpty(StateIdList)) AS LocatinonTargetingFlag
                FROM KWCandidatesWithAuctionPassNegKW
                     LEFT OUTER JOIN
                         CampaignTargetInfo
                     ON KWCandidatesWithAuctionPassNegKW.CampaignId == CampaignTargetInfo.CampaignId;
            '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidatesWithLocationTarget')
        self.assertCountEqual(result['sources'], ['KWCandidatesWithAuctionPassNegKW', 'CampaignTargetInfo'])


    def test_parse_column_if(self):
        s = '''
            KeywordAuctionQualityFactor =
                SELECT A.*,
                       IF(B.QualityFactorScale == NULL, 1.0, B.QualityFactorScale) AS QualityFactorScale,
                       IF(B.PClickScale == NULL, 1.0, B.PClickScale) AS PClickScale,
                       QualityFactor * QualityFactorScale * SuggBid AS NewRS,
                       PClick * PClickScale AS NewPClick
                FROM KeywordAuctionQualityFactor AS A
                     LEFT OUTER JOIN
                         QualityFactorScale AS B
                     ON A.OrderId == B.AdGroupId;
            '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KeywordAuctionQualityFactor')
        self.assertCountEqual(result['sources'], ['KeywordAuctionQualityFactor', 'QualityFactorScale'])

    def test_parse_aggr_over(self):
        s = '''
            KWCandidatesWithRS = SELECT *,
                                        COUNT() OVER (PARTITION BY OrderId, SuggKW, OptType) AS Srpv
                FROM KWCandidatesWithRS;
            '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidatesWithRS')
        self.assertCountEqual(result['sources'], ['KWCandidatesWithRS'])

    def test_parse_semijoin(self):
        s = '''
            ListingBidDemand =
                SELECT A.*
                FROM (SSTREAM @ListingBidDemand) AS A
                     LEFT SEMIJOIN
                         AuctionContextOnlyWithTA
                     ON A.RGUID == AuctionContextOnlyWithTA.RGUID;
            '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'ListingBidDemand')
        self.assertCountEqual(result['sources'], ['SSTREAM_@ListingBidDemand', 'AuctionContextOnlyWithTA'])

    def test_parse_cross_apply(self):
        s = '''
            ListingBidDemand =
                SELECT DISTINCT RGUID,
                       (long) ListingId AS ListingId,
                       L.TotalPosition AS Position,
                       L.Clicks
                FROM ListingBidDemand AS A
                     CROSS APPLY
                         BondExtension.Deserialize<BidLandscape>(A.SimulationResult).BidPoints AS L;
            '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'ListingBidDemand')
        self.assertCountEqual(result['sources'], ['ListingBidDemand', 'BOND_BondExtension.Deserialize<BidLandscape>(A.SimulationResult).BidPoints'])

    def test_parse_union_all(self):
        s = '''
                AllStat =
                    SELECT "OrderIdCount" AS Tag,
                           COUNT(DISTINCT (OrderId)) AS Num
                    FROM BroadMatchOptDedup
                    UNION ALL
                    SELECT "SuggCount" AS Tag,
                           COUNT( * ) AS Num
                    FROM BroadMatchOptDedup
                    UNION ALL
                    SELECT "AccountIdCount" AS Tag,
                           COUNT(DISTINCT (AccountId)) AS Num
                    FROM BroadMatchOptDedup
                '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AllStat')
        self.assertCountEqual(result['sources'], ['BroadMatchOptDedup'])

    def test_parse_complex_and(self):
        s = '''
KWCandidatesWithLocationTarget =
    SELECT KWCandidatesWithAuctionPassNegKW.AccountId,
           CampaignTargetInfo.IsLocationBidAdjustmentEnabled,
           !(string.IsNullOrEmpty(CountryIdList) AND string.IsNullOrEmpty(StateIdList) AND string.IsNullOrEmpty(MetroAreaIdList) AND string.IsNullOrEmpty(CityIdList)) AS LocatinonTargetingFlag
    FROM KWCandidatesWithAuctionPassNegKW
         LEFT OUTER JOIN
             CampaignTargetInfo
         ON KWCandidatesWithAuctionPassNegKW.CampaignId == CampaignTargetInfo.CampaignId;

                '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidatesWithLocationTarget')
        self.assertCountEqual(result['sources'], ['KWCandidatesWithAuctionPassNegKW', 'CampaignTargetInfo'])


    def test_column_ternary(self):
        s = '''
        AuctionCostCompare =
            SELECT OrderId,
                   B.Cost AS OldCost,
                   (NewCost == - 1? B.Cost : NewCost) AS NewCost
            FROM AuctionNewCost AS A
                 INNER JOIN
                     AuctionContextOnlyWithTA AS B
                 ON A.RGUID == B.RGUID;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AuctionCostCompare')
        self.assertCountEqual(result['sources'], ['AuctionNewCost', 'AuctionContextOnlyWithTA'])

    def test_column_new_obj(self):
        s = '''
        KWCandidatesWithNewPos = SELECT RGUID, new KeywordOptNode(AccountId, CampaignId, OrderId, OptType, SuggKW, KeyTerm, SuggBid, NewRS, NewPos, NewPClick) AS KWONode
        FROM KWCandidatesWithNewPos;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidatesWithNewPos')
        self.assertCountEqual(result['sources'], ['KWCandidatesWithNewPos'])

    def test_cross_join(self):
        s = '''
        BadDump = 
            SELECT BMMOptAfterCapping.*,
                   BadKeywordsPM
            FROM BMMOptAfterCapping
            CROSS JOIN LinePM;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'BadDump')
        self.assertCountEqual(result['sources'], ['BMMOptAfterCapping', 'LinePM'])

    def test_cast_stmt(self):
        s = '''
        TMAllData =
            SELECT Keyword,
                    MatchTypeId,
                    (double) SUM(Impressions * CompetitiveIndex) / SUM(Impressions) AS CompetitiveIndex
            FROM TMAllData;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'TMAllData')
        self.assertCountEqual(result['sources'], ['TMAllData'])

    def test_cast_space_num(self):
        s = '''
        TMAllData =
            SELECT Keyword,
                   MatchTypeId,
                   (long) - 1 AS CompetitiveIndex
            FROM TMAllData;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'TMAllData')
        self.assertCountEqual(result['sources'], ['TMAllData'])


    def test_multiple_join(self):
        s = '''
        FullSuggestions =
            SELECT A. *,
                   B.SuggBid AS AggSuggBid,
                   D.NumAdGroups
            FROM FullSuggestions AS A
                 INNER JOIN
                     AggregatedEstimation AS B
                 ON A.AccountId == B.AccountId AND A.CampaignId == B.CampaignId AND A.OrderId == B.OrderId
                 LEFT OUTER JOIN
                     AdgroupCount AS D
                 ON A.AccountId == D.AccountId AND A.CampaignId == D.CampaignId AND A.OrderId == D.OrderId;

        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'FullSuggestions')
        self.assertCountEqual(result['sources'], ['FullSuggestions', 'AggregatedEstimation', 'AdgroupCount'])

    def test_multiple_select_union(self):
        s = '''
        AdgroupCount =
            SELECT AccountId,
                   CampaignId
            FROM AdgroupCountSource
            UNION ALL
            SELECT AccountId,
                   NumAdGroups
            FROM CampaignLevelAdgroupCount
            UNION ALL
            SELECT AccountId, 
                  (long) -1 AS CampaignId, 
                  (long) -1 AS OrderId, 
                  NumAdGroups 
            FROM AccountLevelAdgroupCount;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AdgroupCount')
        self.assertCountEqual(result['sources'], ['AdgroupCountSource', 'CampaignLevelAdgroupCount', 'AccountLevelAdgroupCount'])





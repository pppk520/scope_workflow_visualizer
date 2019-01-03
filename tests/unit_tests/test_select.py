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

    def test_parse_from_input_module_freestyle(self):
        s = '''
        DiagnosticFull = 
            SELECT * 
            FROM DiagnosticModules.DiagnosticPageView(INPUT_BASE = @CommomDataLayerBasePath, END_DATE_UTC = @RunDateTimeUTC) 
             
            WHERE NOT (Mainline4Reserve == null AND MainlineReserve == null AND ABTestConfigVersion == null AND NumSidebarAds == null)
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'DiagnosticFull')
        self.assertCountEqual(result['sources'], ['MODULE_DiagnosticModules.DiagnosticPageView'])

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
        self.assertCountEqual(result['sources'], ['ListingBidDemand', 'FUNC_BondExtension.Deserialize<BidLandscape>(A.SimulationResult).BidPoints'])

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

    def test_complex_if_column(self):
        s = '''
        AccountsWithTrafficId = 
            SELECT A.AccountId, IF(B.TrafficId!=null,
                                    (int)B.TrafficId,
                                    IF(C.TrafficId!=null,
                                        (int)C.TrafficId,
                                        1<<((new Random(Guid.NewGuid().GetHashCode()).Next(0,4))+3)//If no bucket, random assign bucket
                                    )
                                   ) AS TrafficId 
            FROM Accounts AS A
            LEFT OUTER JOIN BadAccounts AS B 
            ON A.AccountId==B.AccountId
            LEFT OUTER JOIN AccountBucket AS C
            ON A.AccountId==C.AccountId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AccountsWithTrafficId')
        self.assertCountEqual(result['sources'], ['Accounts', 'BadAccounts', 'AccountBucket'])

    def test_complex_if_2(self):
        s = '''
        OrderMPISpend =   SELECT OrderMPISpend.*,
                                 IF(DailyBudgetUSD == null || MPISpend/100.0 <= DailyBudgetUSD, 1.0, DailyBudgetUSD/(MPISpend/100.0)) AS BudgetFactor
            FROM OrderMPISpend LEFT OUTER JOIN CampaignBudget ON
                                                                    OrderMPISpend.CampaignId == CampaignBudget.CampaignId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'OrderMPISpend')
        self.assertCountEqual(result['sources'], ['OrderMPISpend', 'CampaignBudget'])


    def test_union_distinct(self):
        s = '''
        BadAccounts =
            SELECT AccountId,
                   (int) 2 AS TrafficId
            FROM AggregatorList WHERE bAggregator==true
            UNION DISTINCT
            SELECT AccountId,
                   (int) 2 AS TrafficId
            FROM SpamAccountList;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'BadAccounts')
        self.assertCountEqual(result['sources'], ['AggregatorList', 'SpamAccountList'])

    def test_union_distinct(self):
        s = '''
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

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'ClickRows_AdvertiserClicks')
        self.assertCountEqual(result['sources'], ['Monetization_Clicks'])

    def test_column_double_question_mark(self):
        s = '''
        Campaigns = 
            SELECT A.*,
                   B.SpendUSD??0 AS SpendUSD
            FROM Campaigns AS A
            LEFT OUTER JOIN CampaignSpending AS B
            ON A.CampaignId == B.CampaignId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'Campaigns')
        self.assertCountEqual(result['sources'], ['Campaigns', 'CampaignSpending'])

    def test_union_select(self):
        s = '''
        BlockRules_Customer =
            SELECT DISTINCT CustomerId
            FROM BlockRules
            WHERE CustomerId != - 1
            UNION DISTINCT
            SELECT DISTINCT EntityId AS CustomerId
            FROM DedupeList
            WHERE EntityLevel == "CID"
            UNION DISTINCT
            SELECT DISTINCT CustomerId
            FROM BlockRules_Account2CustomerId; 
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'BlockRules_Customer')
        self.assertCountEqual(result['sources'], ['BlockRules', 'DedupeList', 'BlockRules_Account2CustomerId'])

    def test_from_view(self):
        s = '''
        RejectionRule = 
            SELECT CustomerId,
                   AccountId
            FROM (VIEW @RejectRuleView)
            WHERE CustomizationConditions.Contains("Deduped:Yes") AND TacticCode IN("IN1-I", "BMM-I", "SM1-I", "BMA-I", "SKW-I"); 
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'RejectionRule')
        self.assertCountEqual(result['sources'], ['VIEW_@RejectRuleView'])

    def test_first_if(self):
        s = '''
        QualityFactorScale =
            SELECT          AdGroupId,
                            FIRST(QualityFactorScale) AS QualityFactorScale,
                            FIRST(IF(double.IsNaN(PClickScale), 1.0, PClickScale)) AS PClickScale
            FROM (SSTREAM @QualityFactorScale);
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'QualityFactorScale')
        self.assertCountEqual(result['sources'], ['SSTREAM_@QualityFactorScale'])

    def test_complex_expr_column(self):
        s = '''
        ExchangeRateMap =
            SELECT CurrencyInfo.CurrencyId,
                   ExchangeRateUSD,
                   (int) Math.Ceiling(MinBid * (ExchangeRateUSD ?? 1m) * 100 - 0.01m) AS MinBid
            FROM CurrencyInfo
                 LEFT OUTER JOIN
                     ExchangeRates
                 ON ExchangeRates.CurrencyId == CurrencyInfo.CurrencyId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'ExchangeRateMap')
        self.assertCountEqual(result['sources'], ['CurrencyInfo', 'ExchangeRates'])

    def test_column_bracket(self):
        s = '''
        PageView =
            SELECT PageView. *,
                   (ExDic.ContainsKey(CurrencyId) ? ExDic[CurrencyId] : 1.0) AS ExchangeRate,
                   MinBids,
                   IF(EnabledMarkets.ContainsKey(TrafficType), EnabledMarkets[TrafficType], null) AS EnabledCountryIds
            FROM PageView
                 CROSS JOIN
                     Configurations;

        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'PageView')
        self.assertCountEqual(result['sources'], ['PageView', 'Configurations'])

    def test_column_parentheses(self):
        s = '''
        PositionBoostMarkets =
            SELECT Market,
                   LIST((CountryCode[0]<< 8) | CountryCode[1]) AS EnabledCountries
            FROM PositionBoostConfig;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'PositionBoostMarkets')
        self.assertCountEqual(result['sources'], ['PositionBoostConfig'])

    def test_column_complex(self):
        s = '''
        ListingMinWinPerf =
            SELECT L.RGUID,
                   L.ListingId,
                   L.BiddedMatchTypeId,
                   L.FilterReason,
                   (L.SimulationResult) [0].Bid AS MinWinBid,
                   (int) (R.AccountWinnerImpressions ?? 0) AS AccountWinnerImpressions,
                   (double) (R.AccountWinnerClicks ?? 0) AS AccountWinnerClicks,
                   (double) (R.AccountWinnerCost ?? 0) AS AccountWinnerCost
            FROM ListingBidDemand AS L
                 LEFT OUTER JOIN
                     AccountWinnerPerf AS R
                 ON L.RGUID == R.RGUID AND L.AdvertiserId == R.AccountId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'ListingMinWinPerf')
        self.assertCountEqual(result['sources'], ['ListingBidDemand', 'AccountWinnerPerf'])


    def test_cross_apply_func(self):
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
        self.assertCountEqual(result['sources'], ['ListingBidDemand', 'FUNC_BondExtension.Deserialize<BidLandscape>(A.SimulationResult).BidPoints'])

    def test_column_comb(self):
        s = '''
        AccountTacticData =
            SELECT AccountId,
                   OptTypeId,
                   AccountId.ToString() + "_" + OptTypeId.ToString() AS TempOpportunityId,
                   OptTacticMapping.OptAdInsightCategory
            FROM AccountTacticData
                 LEFT JOIN
                     AccountInfo
                 ON AccountTacticData.AccountId == AccountInfo.AccountId
                 LEFT JOIN
                     AccountLocationMapping
                 ON AccountTacticData.AccountId == AccountLocationMapping.AccountId
                 LEFT JOIN
                     AccountVerticalMapping
                 ON AccountTacticData.AccountId == AccountVerticalMapping.AccountId
                 LEFT JOIN
                     OptTacticMapping
                 ON AccountTacticData.OptTypeId == OptTacticMapping.OptTypeId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AccountTacticData')
        self.assertCountEqual(result['sources'], ['AccountTacticData', 'AccountInfo', 'AccountLocationMapping', 'AccountVerticalMapping', 'OptTacticMapping'])

    def test_column_func_lambda(self):
        s = '''
        BidHistoryRecord = 
            SELECT OrderItemId,
                   History.Split(';').Select(a => new BidHistory(a)).ToList() AS History
            FROM 
            (
                SSTREAM @BidHistory
            )
            WHERE NOT string.IsNullOrEmpty(History);
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'BidHistoryRecord')
        self.assertCountEqual(result['sources'], ['SSTREAM_@BidHistory'])

    def test_column_op_ident_eq(self):
        s = '''
        Monetization_Ad = 
            SELECT Monetization_Ad.*,
                   OrderId ?? SAOrderId         AS AdGroupId,
                   CountryCode,
                   RawBid == 0 AS UsingDefaultBid
            FROM Monetization_Ad 
            LEFT OUTER JOIN IdMapping
            ON Monetization_Ad.ListingId == IdMapping.OrderItemId
            LEFT OUTER JOIN BidHistoryRecord 
            ON Monetization_Ad.ListingId == BidHistoryRecord.OrderItemId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'Monetization_Ad')
        self.assertCountEqual(result['sources'], ['Monetization_Ad', 'IdMapping', 'BidHistoryRecord'])

    def test_column_nested_func(self):
        s = '''
        AllLevelPerf = 
            SELECT
                Microsoft.RnR.AdInsight.Utils.ConvertToInt(SUM(IF(ALL(ValidImpressions > 0, Utility.AbsPositionFromPagePosition(PagePosition, Markets, LCID, MarketplaceClassificationId, PublisherOwnerId, CountryCode, MLAdsCnt, RequestedMainlineAdsCnt) <= 4), 1, 0))) AS MLImpressions,
                (double)SUM(ConversionCnt) AS Conversions
            FROM BillableListings
            CROSS JOIN PositionBoostConfig
            HAVING LCID >= 0 AND MatchTypeId >= 0 AND MLImpressions >= 0 AND MLImpressionsWithoutAdjust >= 0;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AllLevelPerf')
        self.assertCountEqual(result['sources'], ['BillableListings', 'PositionBoostConfig'])

    def test_column_ident_dot(self):
        s = '''
        PageData = 
            SELECT DISTINCT
                Monetization_PageView.*
            FROM Monetization_PageView 
                LEFT OUTER JOIN Diagnostic AS B
                    ON Monetization_PageView.RGUID == B.RGUID;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'PageData')
        self.assertCountEqual(result['sources'], ['Monetization_PageView', 'Diagnostic'])

    def test_from_sstream_streamset(self):
        s = '''
        MPI =  SELECT *, __serialnum AS N FROM 
                                              (SSTREAM 
                   STREAMSET @MPI_PATH
                   PATTERN @"KeywordOptMPIFinal%n.ss"
                   RANGE __serialnum=["0", "6"]
        );
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'MPI')
        self.assertCountEqual(result['sources'], ['SSTREAM<STREAMSET>_@MPI_PATH'])

    def test_from_inner_select(self):
        s = '''
        DedupeList_BlockRules =
            (SELECT CustomerId AS EntityId
             FROM BlockRules_Customer
             UNION DISTINCT
             SELECT *
             FROM BlockRules_Account
            )
            EXCEPT ALL
            (SELECT EntityId
             FROM DedupeList
             WHERE EntityLevel == "CID"
            )
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'DedupeList_BlockRules')
        self.assertCountEqual(result['sources'], ['BlockRules_Customer', 'BlockRules_Account', 'DedupeList'])

    def test_from_one_line_from(self):
        s = '''
        CampaignInRGUIDsWithName =
            SELECT A.*,B.CampaignName FROM CampaignInRGUIDs AS A LEFT OUTER JOIN NoSLCampaigns AS B
            ON A.CampaignId == B.CampaignId
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'CampaignInRGUIDsWithName')
        self.assertCountEqual(result['sources'], ['CampaignInRGUIDs', 'NoSLCampaigns'])

    def test_complex_column_multiline(self):
        s = '''
        AuctionWithUpdatedPclick =
            SELECT OptId,
                   CampaignId,
                   A.RGUID,
                   A.ListingId,
                   A.CPC,
                   (B.Position == NULL
                   ? (AuctionSimulator.SimulateCtx.ActualClicks.Any() && AuctionSimulator.SimulateCtx.Competitors.Values.Where(p=>p.ListingId == (ulong)ListingId).FirstOrDefault()!= NULL
                   ? Math.Min(1,
                   AuctionSimulator.SimulateCtx.ActualClicks.Sum(a => a.GetClicks(
                   AuctionSimulator.SimulateCtx.Competitors.Values.Where(p => p.ListingId == (ulong) ListingId).FirstOrDefault(),
                   Level.ListingLevel,
                   PClick,
                   (int) Position,
                   AuctionSimulator.SimulateCtx.TheAuction.GetPosition((ulong) ListingId))
                   ))
                   : 0)
                   : B.Clicks) AS PClick
            FROM AuctionWithUpdatedCPC AS A
                 LEFT OUTER JOIN
                     ListingBidDemand AS B
                 ON A.RGUID == B.RGUID
                    AND A.ListingId == B.ListingId
                    AND A.Position == B.Position;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AuctionWithUpdatedPclick')
        self.assertCountEqual(result['sources'], ['AuctionWithUpdatedCPC', 'ListingBidDemand'])


    def test_column_not_in_param_ident(self):
        s = '''
        UsagePattern =
            SELECT *
            FROM UsagePattern
            WHERE OptTypeId NOT IN(@GoogleImportOpportunity, @GoogleImportScheduledOpportunity)
            UNION ALL
            SELECT *
            FROM GICountingFeatures1
            UNION ALL
            SELECT
                *
            FROM GICountingFeatures2;

        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'UsagePattern')
        self.assertCountEqual(result['sources'], ['UsagePattern', 'GICountingFeatures1', 'GICountingFeatures2'])

    def test_form_extract(self):
        s = '''
        EmptyManifest = SELECT COUNT() AS C FROM (EXTRACT A:string FROM @EmptyTxt USING DefaultTextExtractor)
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'EmptyManifest')
        self.assertCountEqual(result['sources'], ['EXTRACT_@EmptyTxt'])

    def test_union_outside_select(self):
        s = '''
        AdInsightUsage =
            (SELECT AdInsightUsage. *
             FROM AdInsightUsage
                  LEFT OUTER JOIN
                      CookedPuLogSupportedV1Operations
                  ON AdInsightUsage.OperationName == CookedPuLogSupportedV1Operations.OperationName
             WHERE CookedPuLogSupportedV1Operations.OperationName == null 
            )
            UNION ALL
            SELECT * FROM BTEBulkApiUsage
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'AdInsightUsage')
        self.assertCountEqual(result['sources'], ['AdInsightUsage', 'CookedPuLogSupportedV1Operations', 'BTEBulkApiUsage'])

    def test_column_new_class_init(self):
        s = '''
        BidOpportunityBond=
            SELECT 
                BidResult.AccountId,
                CampaignId,
                OrderId,
                OptType,
                new BidOpportunityNode{
                    AccountId = (int)BidResult.AccountId,
                    CollectionOfOpportunityAtChildLevel = BidOpportunities,
                    CurrencyId = CurrencyId ?? 0
                    } AS BidOptNode,
                AccountPartitioner.GetPartitionId(BidResult.AccountId, @RunDateTime) AS PartitionId
            FROM BidOpportunityResult AS BidResult
            LEFT OUTER JOIN AccountCurrencyId AS AcctCurrency
            ON BidResult.AccountId == AcctCurrency.AccountId;
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'BidOpportunityBond')
        self.assertCountEqual(result['sources'], ['BidOpportunityResult', 'AccountCurrencyId'])

    def test_column_func_complex(self):
        s = '''
        landscapeResult = 
            SELECT 
                listingBidTrafficRounded.ListingId, 
                listingBidTrafficRounded.CreatedDtim,
                listingBidTrafficRounded.AdGroupId,
                listingBidTrafficRounded.CampaignId,
                listingBidTrafficRounded.AccountId,
                Landscape.GetBidLandscape(LatestBidUSCent ?? -1, new LandscapePointSelectionConfig())
                .CompressCurve(LatestBidUSCent ?? -1, new CurveCompressionConfig()) AS Landscape
            FROM listingBidTrafficRounded
            LEFT OUTER JOIN latestBids 
            ON listingBidTrafficRounded.ListingId == latestBids.AutoTargetId;

        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'landscapeResult')
        self.assertCountEqual(result['sources'], ['listingBidTrafficRounded', 'latestBids'])


    def test_column_list_new_class(self):
        s = '''
        Impressions =
            SELECT RGUID,
                   SUM(AmountChargedUSDMonthlyExchangeRt * ValidClicks * 100) AS Cost,
                   LIST(new AdListing {
                   ListingId = ListingId,
                   ActualBid = (uint) (ActualBidAmtUSD * 100 + 0.5m),
                   CPC = (double) (IF(PagePosition.StartsWith("ML"), CPC_ML, CPC_SB) * ActualBidAmtUSD * 100 / ActualBid), // CPC converted from auction currency to USD
                   }) AS AdListings
            FROM Monetization
            WHERE ALL(
                  CustomerId > 0, AdType > 0, MatchTypeId IN(1, 2, 3, 4), BiddedMatchTypeId IN(1, 2, 3),
                  NOT string.IsNullOrEmpty(PagePosition)
                  );        
        
        '''

        result = Select().parse(s)

        self.assertTrue(result['assign_var'] == 'Impressions')
        self.assertCountEqual(result['sources'], ['Monetization'])






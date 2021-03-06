from unittest import TestCase
from scope_parser.output import Output

class TestOutput(TestCase):
    def test_basic(self):
        s = '''
        OUTPUT data TO SSTREAM "/local/tt.ss"
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['data'])
        self.assertTrue(result['path'] == '"/local/tt.ss"')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_stream_expiry(self):
        s = '''
        OUTPUT AuctionMonetizationValidate TO SSTREAM @PubilsherBumpedAuction WITH STREAMEXPIRY @StreamExpiry
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['AuctionMonetizationValidate'])
        self.assertTrue(result['path'] == '@PubilsherBumpedAuction')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_stream_hash_clustered(self):
        s = '''
        OUTPUT TO SSTREAM "@@TopAdvReport@@"
                               HASH CLUSTERED BY ListingFilterReason
                               WITH STREAMEXPIRY "15";
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], [])
        self.assertTrue(result['path'] == '"@@TopAdvReport@@"')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_stream_partition(self):
        s = '''
        OUTPUT IS_ORDERITEM TO @ImpressionShareReportOrderItem
                               PARTITIONED BY StripePartitionId
                               WITH STREAMEXPIRY @EXPIRY
                               USING CSVFileOutputter(@METADATA_AGG_DSV, "ImpressionShareData_OrderItem", @ASCIICsvXfmArgs_ISOutputs)
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['IS_ORDERITEM'])
        self.assertTrue(result['path'] == '@ImpressionShareReportOrderItem')
        self.assertTrue(result['stream_type'] is None)
        self.assertCountEqual(result['attributes'], ['PARTITION'])

    def test_stream_sort(self):
        s = '''
        OUTPUT BIOrderItem
        TO SSTREAM @BIContact
           CLUSTERED BY OrderItemId
               SORTED BY OrderItemId
           WITH STREAMEXPIRY @StreamExpiryDays;
        '''

        result = Output().parse(s)


        self.assertCountEqual(result['idents'], ['BIOrderItem'])
        self.assertTrue(result['path'] == '@BIContact')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_stream_from_select(self):
        s = '''
        OUTPUT (SELECT * FROM Suggestions WHERE bNeedFiltered == true) TO SSTREAM @Output_FilteredBy_SpecialRuleForCustomerFilter CLUSTERED BY OrderId WITH STREAMEXPIRY @STREAM_EXPIRY
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['Suggestions'])
        self.assertTrue(result['path'] == '@Output_FilteredBy_SpecialRuleForCustomerFilter')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_stream_from_select_top(self):
        s = '''
        OUTPUT (SELECT TOP 1 *) TO @OutputFile WITH STREAMEXPIRY @STREAM_EXPIRY USING SchemaOutputter
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], [])
        self.assertTrue(result['path'] == '@OutputFile')
        self.assertTrue(result['stream_type'] is None)

    def test_ident_dot(self):
        s = '''
        OUTPUT Results_SearchPerf.LostToBudgetRatio TO SSTREAM @RatioOfAuctionLostToBudget_Search CLUSTERED BY CampaignId WITH STREAMEXPIRY "30"
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['Results_SearchPerf'])
        self.assertTrue(result['path'] == '@RatioOfAuctionLostToBudget_Search')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_select_union(self):
        s = '''
        OUTPUT
        (
            SELECT * FROM BudgetLandscape_Backward
            UNION ALL
            SELECT * FROM BudgetLandscape_Forward
            UNION ALL
            SELECT * FROM KKK
        )
        TO SSTREAM @OutputBudgetLandscape
        CLUSTERED BY CampaignId
        WITH STREAMEXPIRY @StreamExpiry;
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['BudgetLandscape_Backward', 'BudgetLandscape_Forward'])
        self.assertTrue(result['path'] == '@OutputBudgetLandscape')
        self.assertTrue(result['stream_type'] == 'SSTREAM')

    def test_using_to(self):
        s = '''
        OUTPUT BudgetSuggestionsAll USING A.B.Opportunities("aa", "bb") TO @BudgetRecsAllPath
                WITH STREAMEXPIRY @STREAM_EXPIRY
        '''

        result = Output().parse(s)

        self.assertCountEqual(result['idents'], ['BudgetSuggestionsAll'])
        self.assertTrue(result['path'] == '@BudgetRecsAllPath')




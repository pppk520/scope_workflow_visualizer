from unittest import TestCase
from scope_parser.reduce import Reduce

class TestReduce(TestCase):
    def test_reduce_on(self):
        s = '''
        PkvBond =
            REDUCE FullSuggestions
            ON AccountId, CampaignId, OrderId
            USING KWOptPkvTableProposalReducer("Opportunity", "Opportunity", "Opportunity");
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'PkvBond')
        self.assertCountEqual(result['sources'], ['FullSuggestions'])

    def test_reduce_using(self):
        s = '''
        Suggestions =
            REDUCE Suggestions
            USING AccountLevelSourceReducer(@CompetitorTrackId, @CappedNum)
            ON AccountId
            PRESORT Score DESC
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'Suggestions')
        self.assertCountEqual(result['sources'], ['Suggestions'])
        self.assertTrue(result['using'] == 'AccountLevelSourceReducer')

    def test_presort_using(self):
        s = '''
        KWCandidatesWithRS = REDUCE KWCandidatesWithRS ON
                                    OrderId,SuggKW,OptType
                                    PRESORT Rank
            USING RGUIDSampleReducer()
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'KWCandidatesWithRS')
        self.assertCountEqual(result['sources'], ['KWCandidatesWithRS'])
        self.assertTrue(result['using'] == 'RGUIDSampleReducer')

    def test_produce_using(self):
        s = '''
        CompressDetailBroadMatchOpt =
            REDUCE DetailBroadMatchOpt
            PRODUCE AccountID,
                    CampaignID,
                    AIPartitionId
            USING DetailBMOptBondReducer(@creationDate, @expiryDate)
            ON AccountId
            PRESORT BMImps DESC
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'CompressDetailBroadMatchOpt')
        self.assertCountEqual(result['sources'], ['DetailBroadMatchOpt'])
        self.assertTrue(result['using'] == 'DetailBMOptBondReducer')

    def test_presort_asc_desc(self):
        s = '''
        Suggestions =
            REDUCE Suggestions
            ON Key
            PRESORT AccountId ASC, Score DESC, MonthlyQueryVolume DESC
            USING GroupRankReducer("1", @MaxSuggCount)
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'Suggestions')
        self.assertCountEqual(result['sources'], ['Suggestions'])
        self.assertTrue(result['using'] == 'GroupRankReducer')

    def test_from_select(self):
        s = '''
        Suggestions =
            REDUCE
            (
                SELECT AccountId,
                       OrderId,
                       Score
                FROM Suggestions
            )
            ON OrderId
            PRESORT Score DESC
            USING Utils.TopNReducer(@OrderSuggestionCount)
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'Suggestions')
        self.assertCountEqual(result['sources'], ['Suggestions'])
        self.assertTrue(result['using'] == 'Utils.TopNReducer')

    def test_any_order(self):
        s = '''
        CampaignTarget = REDUCE CampaignTarget
                         PRESORT CurrentGeoLevel ASC
                         ON CampaignId
                         USING CampaignTargetReducer()
        '''

        result = Reduce().parse(s)

        self.assertTrue(result['assign_var'] == 'CampaignTarget')
        self.assertCountEqual(result['sources'], ['CampaignTarget'])
        self.assertTrue(result['using'] == 'CampaignTargetReducer')




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



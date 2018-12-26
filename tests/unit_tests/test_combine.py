from unittest import TestCase
from scope_parser.combine import Combine

class TestCombine(TestCase):
    def test_combine_on(self):
        s = '''
        SuggestionsWithScore =
        COMBINE Suggestions AS L WITH OrderTermBag AS R
        ON L.OrderId == R.OrderId
        USING SuggestionTfCombiner("TFIDF");
        '''

        result = Combine().parse(s)

        self.assertTrue(result['assign_var'] == 'SuggestionsWithScore')
        self.assertCountEqual(result['sources'], ['Suggestions', 'OrderTermBag'])
        self.assertTrue(result['using'] == 'SuggestionTfCombiner')


    def test_combine_with_sstream(self):
        s = '''
        OrderBiddingKW =
            COMBINE OrderBiddingKW AS L WITH (SSTREAM@OrderTFScoreFile) AS R
            ON L.OrderId == R.OrderId
            USING SuggestionTfCombiner("TFIDF")
            '''

        result = Combine().parse(s)

        self.assertTrue(result['assign_var'] == 'OrderBiddingKW')
        self.assertCountEqual(result['sources'], ['OrderBiddingKW', '(SSTREAM@OrderTFScoreFile)'])
        self.assertTrue(result['using'] == 'SuggestionTfCombiner')




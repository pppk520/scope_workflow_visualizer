c
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



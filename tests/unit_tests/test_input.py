from unittest import TestCase
from scope_parser.input import Input

class TestSelect(TestCase):
    def test_view(self):
        s = '''
        TMAllData = 
            VIEW @TMView
            PARAMS(
                START_DATE = @TM_START_DATE,
                END_DATE = @TM_END_DATE
            );
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == 'TMAllData')
        self.assertCountEqual(result['sources'], ['VIEW_@TMView'])

    def test_module(self):
        s = '''
        Data =
            KWO.AssignTraffficId
            (
                Input = MergedSources_Final,
                Config = AccountsWithTrafficId,
                Mode = "config"
            );
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == 'Data')
        self.assertCountEqual(result['sources'], ['MODULE_KWO.AssignTraffficId'])
        self.assertCountEqual(result['params'], ['MergedSources_Final', 'AccountsWithTrafficId', '"config"'])


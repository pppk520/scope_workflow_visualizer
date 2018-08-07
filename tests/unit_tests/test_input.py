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
        self.assertTrue(result['from_source'] == '@TMView')

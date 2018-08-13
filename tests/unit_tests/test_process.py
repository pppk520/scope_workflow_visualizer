from unittest import TestCase
from scope_parser.process import Process

class TestProcess(TestCase):
    def test_process_using_func_param(self):
        s = '''
        IS_ORDER_BSC = PROCESS IS_ORDER_BSC USING StripePartitionLookupProcessor("PipelineConfiguration.xml")
        '''

        result = Process().parse(s)

        self.assertTrue(result['assign_var'] == 'IS_ORDER_BSC')
        self.assertCountEqual(result['sources'], ['IS_ORDER_BSC'])

    def test_process_using_func_ptr(self):
        s = '''
        BMMOpt =
            PROCESS BMMOpt
            USING ProcessExistingPlus
        '''

        result = Process().parse(s)

        self.assertTrue(result['assign_var'] == 'BMMOpt')
        self.assertCountEqual(result['sources'], ['BMMOpt'])


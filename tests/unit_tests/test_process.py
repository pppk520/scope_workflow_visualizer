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

    def test_process_using_having(self):
        s = '''
        AuctionWithUpdatedPclick = PROCESS KWCandidatesPrepared PRODUCE AccountId, CampaignId, OrderId, OptType, SuggKW, KeyTerm, RGUID, ListingId, CPC, PClick
            USING BTEAdjustmentProcessor()
            HAVING RGUID != "";

        '''

        result = Process().parse(s)

        self.assertTrue(result['assign_var'] == 'AuctionWithUpdatedPclick')
        self.assertCountEqual(result['sources'], ['KWCandidatesPrepared'])
        self.assertTrue(result['using'], 'BTEAdjustmentProcessor')


    def test_process_no_assign(self):
        s = '''
        PROCESS USING BSCPartitionLookup(@BSCPartitionHistogramFileName ); 
        '''

        result = Process().parse(s)

        self.assertTrue(result['assign_var'] is None)
        self.assertCountEqual(result['sources'], [])
        self.assertTrue(result['using'], 'BSCPartitionLookup')

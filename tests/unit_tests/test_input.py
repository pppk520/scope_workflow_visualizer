from unittest import TestCase
from scope_parser.input import Input

class TestInput(TestCase):
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


    def test_extract(self):
        s = '''
        LinePM = 
            EXTRACT BadKeyword:string 
            FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt" 
            USING DefaultTextExtractor();
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == 'LinePM')
        self.assertCountEqual(result['sources'], ['EXTRACT_"/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt"'])

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

    def test_module_2(self):
        s = '''
        Data = MonetizationModules.MonetizationClick(
            INPUT_BASE = "a",
            START_DATETIME_UTC = @StartDateTime_DT,
            END_DATETIME_UTC = @EndDateTime_DT
        )
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == 'Data')
        self.assertCountEqual(result['sources'], ['MODULE_MonetizationModules.MonetizationClick'])
        self.assertCountEqual(result['params'], ['"a"', '@StartDateTime_DT', '@EndDateTime_DT'])



    def test_sstream(self):
        s = '''
        OrderNegativeKeyword = SSTREAM @OrderNegativeKeyword
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == "OrderNegativeKeyword")
        self.assertCountEqual(result['sources'], ['SSTREAM_@OrderNegativeKeyword'])


    def test_import(self):
        s = '''
        IMPORT "/somewhere/KeywordRelevanceFeature.script" AS KeywordRevelanceData
        PARAMS PROCESS_DATE = @PROCESS_DATE;    
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == "KeywordRevelanceData")
        self.assertCountEqual(result['sources'], ['IMPORT_"/somewhere/KeywordRelevanceFeature.script"'])

    def test_extract_column_nullable(self):
        s = '''
        AccountInfo =
            EXTRACT AccountId : int?,
                    Acct_Num : string,
                    Org : string,
                    Service : string,
                    Svc_Segment : string,
                    AdCenter_Svc_Lvl : string,
                    FinancialStatus : string
            FROM @AllAccountInfoFile
            USING DefaultTextExtractor(silent: true)
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == "AccountInfo")
        self.assertCountEqual(result['sources'], ['EXTRACT_@AllAccountInfoFile'])

    def test_module_func_ptr(self):
        s = '''
        NKWResult =
            NKWAPI.KeywordBidEstimation
            (
                KeywordAdvertiser = OrderSuggKW_CrossLCID,
                CampaignLocationTarget = NKWData.CampaignLocationTarget,
                EnableMM = true
            );
        )
        '''

        result = Input().parse(s)

        self.assertTrue(result['assign_var'] == 'NKWResult')
        self.assertCountEqual(result['sources'], ['MODULE_NKWAPI.KeywordBidEstimation'])
        self.assertCountEqual(result['params'], ['OrderSuggKW_CrossLCID', 'NKWData.CampaignLocationTarget', 'true'])




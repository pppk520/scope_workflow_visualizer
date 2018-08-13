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

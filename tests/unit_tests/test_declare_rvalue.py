from unittest import TestCase
from scope_parser.declare_rvalue import DeclareRvalue

class TestDeclareRvalue(TestCase):
    def test_func_chain(self):
        s = '''
        DateTime.Parse("@@RunDate@@")
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['DateTime.Parse("@@RunDate@@")'])

    def test_param_str(self):
        s = '''
        @"@@KWRawPath@@"; //comment'
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['@"@@KWRawPath@@"'])

    def test_func_param_str_cat(self):
        s = '''
        DateTime.Parse("2018" + " " + "20" + ":00:00") 
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['DateTime.Parse("2018 20:00:00")'])


    def test_func_chain_int_parse(self):
        s = '''
        int.Parse("@@AuctionDataGetRatio@@");
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['int.Parse("@@AuctionDataGetRatio@@")'])

    def test_nums(self):
        s = '''
        66
        '''

        result = DeclareRvalue().parse(s)
        self.assertEqual(result['format_items'][0], '66')

    def test_str_cat(self):
        s = '''
        "abc" + "111"
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['"abc"', '+', '"111"'])

    def test_str_cat2(self):
        s = '''
        "abc" + "111" + "222" + "333"
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['"abc"', '+', '"111"', '+', '"222"', '+', '"333"'])


    def test_str_format_item_fun(self):
        s = '''
        string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate));
        '''

        result = DeclareRvalue().parse(s)
        self.assertTrue(result['format_str'] == '{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss')
        self.assertCountEqual(result['format_items'], ['@KWRawPath', 'DateTime.Parse(@RunDate)'])

    def test_str_format_query_param(self):
        s = '''
        string.Format("{0}/BidEstimation/Result/%Y/%m/KeywordAuction_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", @BTEPath, @BTERunDate);
        '''

        result = DeclareRvalue().parse(s)
        self.assertTrue(result['format_str'] == '{0}/BidEstimation/Result/%Y/%m/KeywordAuction_%Y-%m-%d.ss?date={1:yyyy-MM-dd}')
        self.assertCountEqual(result['format_items'], ['@BTEPath', '@BTERunDate'])

    def test_str_format_item_str(self):
        s = '''
        string.Format("{0}/{1:yyyy/MM/dd}/Delta{2}_", @"/local/prod/pipelines/Optimization/KeywordOpportunity/Preparations/MPIProcessing/Debug", DateTime.Parse(@RunDate), @DateDelta); 
        '''

        result = DeclareRvalue().parse(s)
        self.assertTrue(result['format_str'] == '{0}/{1:yyyy/MM/dd}/Delta{2}_')
        self.assertCountEqual(result['format_items'], ['@"/local/prod/pipelines/Optimization/KeywordOpportunity/Preparations/MPIProcessing/Debug"','DateTime.Parse(@RunDate)', '@DateDelta'])


    def test_ref_func_chain(self):
        s = '''
        @RunDate.AddDays(-6).ToString("yyyy-MM-dd") 
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['@RunDate.AddDays(-6).ToString("yyyy-MM-dd")'])

    def test_format_str_param(self):
        s = '''
        string.Format(@IdNamePath, @BidOptFolder, @RunDateTime, "OrderItemIdNameMap") 
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['@BidOptFolder', '@RunDateTime', '"OrderItemIdNameMap"'])

    def test_bool(self):
        s = '''
        false 
        '''

        result = DeclareRvalue().parse(s)
        self.assertCountEqual(result['format_items'], ['false'])



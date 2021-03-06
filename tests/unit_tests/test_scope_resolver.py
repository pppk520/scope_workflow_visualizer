from unittest import TestCase
from myparser.scope_resolver import ScopeResolver
from dateutil import parser
from datetime import datetime

#import logging
#logging.basicConfig(level=logging.DEBUG)

class TestScopeResolver(TestCase):
    def setUp(self):
        self.declare_map = {
            '@DateDelta': 'Math.Abs(5).ToString();',
            '@BTEPath': 'the_bte_path',
            '@BTERunDate': '2018-08-01',
            '@RunDate': '2018-01-02',
            '@dateObj': parser.parse('2018-01-01'),
            '@KWRawPath': 'kw_raw_path'
        }

    def test_basic_num(self):
        items = ["66"]

        result = ScopeResolver().resolve_basic(items, {})
        self.assertEqual(66, result)

    def test_basic_str_cat(self):
        items = ['"abc"', '+', '"123"']

        result = ScopeResolver().resolve_basic(items, {'"123"': '"ABC"'})
        self.assertEqual("abcABC", result)

    def test_str_format(self):
        fmt_str = '/{0}/{1}-{2}'
        items = ['AAA', 'BBB', 'CCC']

        result = ScopeResolver().resolve_str_format(fmt_str, items, {})
        self.assertEqual("/AAA/BBB-CCC", result)

    def test_func_int_parse(self):
        func_str = 'int.Parse("1000")'

        result = ScopeResolver().resolve_func(func_str)
        self.assertEqual(1000, result)

    def test_func_math_abs(self):
        func_str = 'Math.Abs(-1000)'

        result = ScopeResolver().resolve_func(func_str)
        self.assertEqual(1000, result)

    def test_func_datetime_parse(self):
        func_str = 'DateTime.Parse("2011-01-01")'

        result = ScopeResolver().resolve_func(func_str) # result is datetime obj
        result_str = result.strftime('%Y-%m-%d')
        self.assertEqual("2011-01-01", result_str)

    def test_func_datetime_parse_add_days(self):
        func_str = 'DateTime.Parse("2018-08-01").AddDays(3)'

        result = ScopeResolver().resolve_func(func_str)
        result_str = result.strftime('%Y-%m-%d')
        self.assertEqual("2018-08-04", result_str)

    def test_string_format_datetime_parse(self):
        s = '''
        string.Format("{0}/BidEstimation/Result/%Y/%m/AuctionContext_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", "path_to", DateTime.Parse("2018-08-03"));
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual("path_to/BidEstimation/Result/2018/08/AuctionContext_2018-08-03.ss?date=2018-08-03", result)

    def test_str_cat_datetime_parse_range(self):
        s = '''
        "/path_to/%Y/%m/KeywordsSearchCountDaily_%Y-%m-%d.ss?date=" + @dateObj.AddDays(-31).ToString("yyyy-MM-dd") + "..." + @dateObj.AddDays(-1).ToString("yyyy-MM-dd") + "&sparsestreamset=true"
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual("/path_to/2017/12/KeywordsSearchCountDaily_2017-12-31.ss?date=2017-12-01...2017-12-31&sparsestreamset=true", result)

    def test_string_format_datetime_parse_to_string(self):
        s = '''
        string.Format("/path_to/Daily/%Y/%m/Campaign_FiltrationFunnelDaily_%Y%m%d.ss?date={0}...{1}", @dateObj.AddDays(-6).ToString("yyyy-MM-dd"), @dateObj.ToString("yyyy-MM-dd"));
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual('/path_to/Daily/2018/01/Campaign_FiltrationFunnelDaily_20180101.ss?date=2017-12-26...2018-01-01', result)

    def test_string_format_idx(self):
        s = '''
        string.Format("{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/AuctionWithKeywordAndMT.ss", @KWRawPath, @dateObj)
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual('kw_raw_path/Preparations/MPIProcessing/2018/01/01/AuctionWithKeywordAndMT.ss', result)


    def test_func_datetime_parseexact(self):
        func_str = 'DateTime.ParseExact("2018-01-01" + " 00:00:00", "yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);'

        result = ScopeResolver().resolve_func(func_str) # result is datetime obj
        result_str = result.strftime('%Y-%m-%d %H:%M:%S')
        self.assertEqual("2018-01-01 00:00:00", result_str)

    def test_string_format_cat_opitem(self):
        s = '''
        string.Format("/path/to/data/prod/pipelines/ImpressionShare/Common"+"/%Y/%m/%d/DSAMerge%Y%m%d%h.ss?date={0}&hour={1}","2018-01-01",22/2*2)
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual('/path/to/data/prod/pipelines/ImpressionShare/Common/2018/01/01/DSAMerge2018010100.ss?date=2018-01-01&hour=22', result)


    def test_string_format_item_int(self):
        s = '''
        String.Format("{0}/%Y/%m/%d/EligibleAuctionParticipants_%h.ss?date={1}&hour={2}", @SOVRawBasePath, @DATE_UTC, 23)
        '''

        declare_map = {'@SOVRawBasePath': '/path/to',
                       '@DATE_UTC': '2018-01-01'}

        result = ScopeResolver().resolve_declare_rvalue(None, s, declare_map)
        self.assertEqual('/path/to/2018/01/01/EligibleAuctionParticipants_00.ss?date=2018-01-01&hour=23', result)


    def test_resolve_math_abs(self):
        s = '''
        Math.Abs(-0).ToString()
        '''

        result = ScopeResolver().resolve_func(s)
        self.assertEqual('0', result)

    def test_func_datetime_parse_add_days_minus_blank(self):
        func_str = 'DateTime.Parse("2018-08-04").AddDays( - 3 )'

        result = ScopeResolver().resolve_func(func_str)
        result_str = result.strftime('%Y-%m-%d')
        self.assertEqual("2018-08-01", result_str)


    def test_string_format_add_days_black_minus(self):
        s = '''
        String.Format(@"{0}RawSearchQuery/RawSearchQuery_{1:yyyy-MM-dd}.ss", @INPUT_PATH, @ObjDate.AddDays( - 1))
        '''

        declare_map = {'@INPUT_PATH': '/path/to/',
                       '@ObjDate': parser.parse('2018-01-01')}

        result = ScopeResolver().resolve_declare_rvalue(None, s, declare_map)
        self.assertEqual('/path/to/RawSearchQuery/RawSearchQuery_2017-12-31.ss', result)

    def test_string_format_param_datetime_parse(self):
        s = '''
        string.Format("{0}/{1:yyyy/MM/dd}/NegativeKWCandidates.ss", @InputPath, DateTime.Parse(@RunDate))
        '''

        declare_map = {'@InputPath': '/path/to',
                       '@RunDate': '2018-01-01'}

        result = ScopeResolver().resolve_declare_rvalue(None, s, declare_map)
        self.assertEqual('/path/to/2018/01/01/NegativeKWCandidates.ss', result)

    def test_string_format_param_str_datetime(self):
        s = '''
        string.Format("{0}/Flights/{1:yyyy/MM/dd}/AuctionParticipants{1:yyyyMMdd}.ss", "/path/to", @BTERunDate)
        '''

        declare_map = {'@BTERunDate': parser.parse('2018-01-01')}

        result = ScopeResolver().resolve_declare_rvalue(None, s, declare_map)
        self.assertEqual('/path/to/Flights/2018/01/01/AuctionParticipants20180101.ss', result)

    def test_inner_string_format(self):
        s = '''
        "aaa__" + string.Format("/path/to/data/prod/pipelines/ImpressionShare/Common"+"/%Y/%m/%d/DSAMerge%Y%m%d%h.ss?date={0}&hour={1}","2018-01-01",22/2*2)
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, {})
        self.assertEqual('aaa__/path/to/data/prod/pipelines/ImpressionShare/Common/2018/01/01/DSAMerge2018010100.ss?date=2018-01-01&hour=22', result)

    def test_resolve_set_rvalue(self):
        s = '''
         @FeatureStreamForAccount.Replace(".ss", "_fakeForSanityCheck.ss")
        '''

        result = ScopeResolver().resolve_set_rvalue(s, {'@FeatureStreamForAccount': 'aa.ss'})
        self.assertEqual('aa_fakeForSanityCheck.ss', result)




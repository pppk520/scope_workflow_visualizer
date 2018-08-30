from unittest import TestCase
from myparser.scope_resolver import ScopeResolver
from dateutil import parser
from datetime import datetime

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
        self.assertEqual(result, 66)

    def test_basic_str_cat(self):
        items = ['"abc"', '+', '"123"']

        result = ScopeResolver().resolve_basic(items, {'"123"': '"ABC"'})
        self.assertEqual(result, "abcABC")

    def test_str_format(self):
        fmt_str = '/{0}/{1}-{2}'
        items = ['AAA', 'BBB', 'CCC']

        result = ScopeResolver().resolve_str_format(fmt_str, items, {})
        self.assertEqual(result, "/AAA/BBB-CCC")

    def test_func_int_parse(self):
        func_str = 'int.Parse("1000")'

        result = ScopeResolver().resolve_func(func_str)
        self.assertEqual(result, 1000)

    def test_func_math_abs(self):
        func_str = 'Math.Abs(-1000)'

        result = ScopeResolver().resolve_func(func_str)
        self.assertEqual(result, 1000)

    def test_func_datetime_parse(self):
        func_str = 'DateTime.Parse("2011-01-01")'

        result = ScopeResolver().resolve_func(func_str) # result is datetime obj
        result_str = result.strftime('%Y-%m-%d')
        self.assertEqual(result_str, "2011-01-01")

    def test_func_datetime_parse_add_days(self):
        func_str = 'DateTime.Parse("2018-08-01").AddDays(3)'

        result = ScopeResolver().resolve_func(func_str)
        result_str = result.strftime('%Y-%m-%d')
        self.assertEqual(result_str, "2018-08-04")

    def test_string_format_datetime_parse(self):
        s = '''
        string.Format("{0}/BidEstimation/Result/%Y/%m/AuctionContext_%Y-%m-%d.ss?date={1:yyyy-MM-dd}", "path_to", DateTime.Parse("2018-08-03"));
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual(result, "path_to/BidEstimation/Result/2018/08/AuctionContext_2018-08-03.ss?date=2018-08-03")

    def test_str_cat_datetime_parse_range(self):
        s = '''
        "/path_to/%Y/%m/KeywordsSearchCountDaily_%Y-%m-%d.ss?date=" + @dateObj.AddDays(-31).ToString("yyyy-MM-dd") + "..." + @dateObj.AddDays(-1).ToString("yyyy-MM-dd") + "&sparsestreamset=true"
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual(result, "/path_to/2017/12/KeywordsSearchCountDaily_2017-12-31.ss?date=2017-12-01...2017-12-31&sparsestreamset=true")

    def test_string_format_datetime_parse_to_string(self):
        s = '''
        string.Format("/path_to/Daily/%Y/%m/Campaign_FiltrationFunnelDaily_%Y%m%d.ss?date={0}...{1}", @dateObj.AddDays(-6).ToString("yyyy-MM-dd"), @dateObj.ToString("yyyy-MM-dd"));
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual(result, '/path_to/Daily/2018/01/Campaign_FiltrationFunnelDaily_20180101.ss?date="2017-12-26"..."2018-01-01"')

    def test_string_format_idx(self):
        s = '''
        string.Format("{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/AuctionWithKeywordAndMT.ss", @KWRawPath, @dateObj)
        '''

        result = ScopeResolver().resolve_declare_rvalue(None, s, self.declare_map)
        self.assertEqual(result, 'kw_raw_path/Preparations/MPIProcessing/2018/01/01/AuctionWithKeywordAndMT.ss')




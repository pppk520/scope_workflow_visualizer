from unittest import TestCase
from myparser.scope_resolver import ScopeResolver

class TestScopeResolver(TestCase):
    def setUp(self):
        self.declare_map = {
            '@DateDelta': 'Math.Abs(5).ToString();',
            '@BTEPath': 'the_bte_path',
            '@BTERunDate': '2018-08-01',
            '@RunDate': '2018-01-02'
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


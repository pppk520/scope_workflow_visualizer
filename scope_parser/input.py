from pyparsing import *
from scope_parser.common import Common


class Input(object):
    SSTREAM = Keyword("SSTREAM")
    STREAMSET = Keyword("STREAMSET")
    PATTERN = Keyword("PATTERN")
    RANGE = Keyword("RANGE")
    VIEW = Keyword("VIEW")
    PARAMS = Keyword("PARAMS")
    EXTRACT = Keyword("EXTRACT")
    FROM = Keyword("FROM")
    USING = Keyword("USING")

    extract_data_type = oneOf("string int ulong long short byte")

    ##################################
    # General
    ##################################
    ident = Common.ident
    comment = Common.comment
    func = Common.func
    func_chain = Common.func_chain
    func_param = Common.func_params
    value_str = Common.value_str

    value_pattern = Combine(Optional('@') + quotedString)

    param_assign = ident + '=' + Combine(Optional('@') + (func_chain | value_str))
    param_assign_list = delimitedList(param_assign)
    params = PARAMS + '(' + param_assign_list + ')'
    dot_name = delimitedList(ident, delim='.', combine=True)

    extract_column = Group(ident + ':' + extract_data_type)

    ##################################
    # Different Input Categories
    ##################################
    parentheses_func_param = '(' + func_param + ')'
    streamset_type = oneOf("__datetime __hour __day __year")
    streamset_range = streamset_type + '=' + Combine('[' + delimitedList(value_str) + ']' + Optional(parentheses_func_param))

    streamset = STREAMSET + value_str + PATTERN + value_pattern + RANGE + streamset_range
    sstream_value_streamset = SSTREAM + streamset
    sstream = SSTREAM + value_str('from_source')
    extract = EXTRACT + delimitedList(extract_column) + FROM + value_str('from_source') + Optional(USING + func)
    view = VIEW + Combine(value_str)('from_source') + params
    module = Group(dot_name("module_dotname") + '(' + param_assign_list + ')')

    assign_sstream = Combine(ident)('assign_var') + '=' + sstream('sstream')
    assign_extract = Combine(ident)('assign_var') + '=' + extract('extract')
    assign_module = Combine(ident)('assign_var') + '=' + module('module')
    assign_view = Combine(ident)('assign_var') + '=' + view('view')

    def parse_sstream(self, s):
        return self.sstream.parseString(s)

    def parse_sstream_streamset(self, s):
        return self.sstream_value_streamset.parseString(s)

    def parse_view(self, s):
        return self.view.parseString(s)

    def parse_extract(self, s):
        return self.extract.parseString(s)

    def parse_func_chain(self, s):
        return self.func_chain.parseString(s)

    def parse_module(self, s):
        return self.module.parseString(s)

    def parse(self, s):
        d = {'assign_var': None,
             'sources': set()}

        if 'EXTRACT' in s:
            ret = self.assign_extract.parseString(s)
            d['sources'].add('EXTRACT_' + ret['from_source'])
        elif 'SSTREAM' in s:
            ret = self.assign_sstream.parseString(s)
            d['sources'].add('SSTREAM_' + ret['from_source'])
        elif 'module' in s.lower():
            ret = self.assign_module.parseString(s)
            d['sources'].add('MODULE_' + ret['module']['module_dotname'])
        elif 'VIEW' in s:
            ret = self.assign_view.parseString(s)
            d['sources'].add('VIEW_' + ret['from_source'])
        else:
            raise NotImplementedError('unsupported input type?')

        d['assign_var'] = ret['assign_var']

        return d

    def debug(self):
        pass

if __name__ == '__main__':
    i = Input()

    print(i.parse_func_chain('dateObj.AddDays(-31).ToString("yyyy-MM-dd")'))
    print(i.parse_module('''
        MonetizationModules.MonetizationClick(
            INPUT_BASE = "a",
            START_DATETIME_UTC = @StartDateTime_DT,
            END_DATETIME_UTC = @EndDateTime_DT
        )
    '''))
    print(i.parse_module('''
        MonetizationModules.MonetizationImpression(
            INPUT_BASE = @MonetizationCommonDataPath, 
            START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
            END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
        )
    '''))

    print(i.parse_sstream('SSTREAM @OrderNegativeKeyword'))
    print(i.parse('OrderNegativeKeyword = SSTREAM @OrderNegativeKeyword'))
    print(i.parse_view('''
        VIEW @"/shares/adCenter.RnR.Daily_SLA/data/AdSelection/Logs/SelectionDailyView.view"
        PARAMS
        (
            startDate = @dateObj.AddDays(-31).ToString("yyyy-MM-dd"),
            endDate = @dateObj.AddDays(-2).ToString("yyyy-MM-dd"),
            dataMode = "Standard"
        )
    '''))

    print(i.parse_view('''
        VIEW @OrderItemSOVView
        PARAMS
        (
            RequestStartDateUTC = @StartDateTime.Date.ToString("yyyy-MM-dd"),
            RequestEndDateUTC = @EndDateTime.Date.ToString("yyyy-MM-dd"),
            RequestStartHourUTC = "09",
            RequestEndHourUTC = "12",
            Mode = "PROD"
        )
    '''))

    print(i.parse_extract('''
        EXTRACT BadKeyword:string 
        FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt" 
        USING DefaultTextExtractor();
    '''))
    print(i.parse_extract('''
        EXTRACT BadKeyword:string 
        FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt"; 
    '''))


    print(i.parse('''
     Monetization_Clicks =
        MonetizationModules.MonetizationClick(
            INPUT_BASE = @ExtCommonDataPath,
            START_DATETIME_UTC = @StartDateTime_DT,
            END_DATETIME_UTC = @EndDateTime_DT
        );   
    '''))

    print(i.parse_sstream('''SSTREAM @AuctionInsightMerge_1'''))
    print(i.parse_sstream_streamset('''
        SSTREAM STREAMSET @AuctionInsightPath
                PATTERN @"/%Year/%Month/%Day/YouPerformanceFinal%Hour.ss?%Minute%Second"
                RANGE __datetime = [@StartTimeStr,@EndTimeStr]("01:00:00")    
    '''))

    print(i.parse_sstream('''SSTREAM @Input_Suggestions'''))

    print(i.parse('''LinePM = 
    EXTRACT BadKeyword:string 
    FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt" 
    USING DefaultTextExtractor();
    '''))

    d = i.parse(
    '''
TMAllData = 
    VIEW @TMView
    PARAMS(
        START_DATE = @TM_START_DATE,
        END_DATE = @TM_END_DATE
    );
        ''')
    print(d['assign_var'])
    print(d['sources'])


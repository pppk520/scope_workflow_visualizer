from pyparsing import *
from common import Common


class Input(object):
    SSTREAM = Keyword("SSTREAM")
    VIEW = Keyword("VIEW")
    PARAMS = Keyword("PARAMS")
    EXTRACT = Keyword("EXTRACT")
    FROM = Keyword("FROM")
    USING = Keyword("USING")

    extract_data_type = oneOf("string int ulong long short byte")

    ident = Common.ident
    comment = Common.comment
    func = Common.func
    func_chain = Common.func_chain
    value_str = Common.value_str

    param_assign = ident + '=' + Combine(Optional('@') + (func_chain | value_str))
    param_assign_list = delimitedList(param_assign)
    params = PARAMS + '(' + param_assign_list + ')'
    dot_name = delimitedList(ident, delim='.', combine=True)

    extract_column = Group(ident + ':' + extract_data_type)

    sstream = SSTREAM + Combine(Optional('@') + (ident | quotedString))
    extract = EXTRACT + delimitedList(extract_column) + FROM + value_str + Optional(USING + func)
    view = VIEW + Combine(value_str) + params
    module = Group(dot_name + '(' + param_assign_list + ')')

    assign_sstream = ident + '=' + sstream
    assign_extract = ident + '=' + extract
    assign_module = ident + '=' + module

    def parse_sstream(self, s):
        return self.sstream.parseString(s)

    def parse_view(self, s):
        return self.view.parseString(s)

    def parse_extract(self, s):
        return self.extract.parseString(s)

    def parse_func_chain(self, s):
        return self.func_chain.parseString(s)

    def parse_module(self, s):
        return self.module.parseString(s)

    def parse(self, s):
        return self.assign_sstream.parseString(s)

    def debug(self):
        pass

if __name__ == '__main__':
    i = Input()

    print(i.parse_func_chain('dateObj.AddDays(-31).ToString("yyyy-MM-dd")'))
    print(i.parse_module('''MonetizationModules.MonetizationClick(
        INPUT_BASE = "a",
        START_DATETIME_UTC = @StartDateTime_DT,
        END_DATETIME_UTC = @EndDateTime_DT
    )'''))
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

    print(i.parse_extract('''
        EXTRACT BadKeyword:string 
        FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt" 
        USING DefaultTextExtractor();
    '''))
    print(i.parse_extract('''
        EXTRACT BadKeyword:string 
        FROM "/shares/bingads.algo.prod.adinsights/data/prod/pipelines/Optimization/KeywordOpportunity/BlockListPM.txt"; 
    '''))


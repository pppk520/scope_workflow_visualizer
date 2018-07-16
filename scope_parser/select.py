from pyparsing import *
from scope_parser.common import Common
from scope_parser.input import Input
import json


class Select(object):
    SELECT = Keyword("SELECT")
    FROM = Keyword("FROM")
    WHERE = Keyword("WHERE")
    JOIN = Keyword("JOIN")
    AS = Keyword("AS")
    ON = Keyword("ON")
    UNION = Keyword("UNION")
    DISTINCT = Keyword("DISTINCT")
    HAVING = Keyword("HAVING")
    IF = Keyword("IF")

    cast = Combine('(' + oneOf("long ulong short int byte") + Optional('?') + ')')
    aggr = oneOf("SUM AVG MAX MIN COUNT FIRST")
    comment = Common.comment
    ident = Common.ident
    value_str = Common.value_str
    func = Common.func
    func_chain = Common.func_chain

    E = CaselessLiteral("E")
    binop = oneOf("== = != < > >= <=")
    arith_sign = Word("+-", exact=1)

    # YouOrderItemId == CompOrderItemId?"You" : Domain AS Domain
    extend_ident = delimitedList(ident, delim='.', combine=True)
    ternary_condition_binop = Group(extend_ident + binop + (extend_ident | value_str))
    ternary_condition_func = func_chain
    ternary = (ternary_condition_binop | ternary_condition_func) + '?' + value_str + ':' + value_str

    # AdId??0UL AS AdId
    null_coal = ident + '??' + value_str

    # IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt
    if_stmt = Group(IF + '(' + (ternary_condition_binop | ternary_condition_func) + ',' + value_str + ',' + value_str + ')')

    select_stmt = Forward()
    column_name = (delimitedList(ident | '*', ".", combine=True))
    cast_ident = Group(Optional(cast) + column_name).setName('cast_identifier')
    aggr_ident = Combine(aggr + '(' + (ident | Empty()) + ')')
    as_something = (AS + ident).setName('as_something')

    one_column = Group((aggr_ident | ternary | null_coal | if_stmt | func_chain | cast_ident)('column_name') + Optional(as_something) | '*').setName('one_column')
    column_name_list = Group(delimitedList(one_column))('column_name_list')
    table_name = (delimitedList(ident, ".", combine=True))("table_name")
    table_name_list = delimitedList(table_name + Optional(as_something).suppress()) # AS something, don't care

    union = Group(UNION + Optional('ALL'))

    where_expression = Forward()
    and_ = Keyword("AND")
    or_ = Keyword("OR")
    in_ = Keyword("IN")

    real_num = Combine(Optional(arith_sign) + (Word(nums) + "." + Optional(Word(nums)) |
                                               ("." + Word(nums))) +
                       Optional(E + Optional(arith_sign) + Word(nums)))
    int_num = Combine(Optional(arith_sign) + Word(nums) +
                      Optional(E + Optional("+") + Word(nums)))
    bool_val = oneOf("true false")

    column_rval = real_num | int_num | quotedString | column_name | bool_val # need to add support for alg expressions
    where_condition = Group(
        (column_name + binop + column_rval) |
        (column_name + in_ + Group("(" + delimitedList(column_rval) + ")")) |
        (column_name + in_ + Group("(" + select_stmt + ")")) |
        ("(" + where_expression + ")")
    )
    where_expression << where_condition + ZeroOrMore((and_ | or_ | '&&' | '|') + where_expression)

    join = Group(Optional(oneOf('LEFT RIGHT OUTER INNER')) + JOIN)
    join_stmt = join + table_name("join_table_name") + Optional(AS + ident) + ON + where_expression

    # define the grammar
    union_select = ZeroOrMore(Optional(union) + select_stmt)
    having = HAVING + restOfLine

    from_select = Group('(' + select_stmt + ')')
    from_module = Group('(' + Input.module + ')')
    from_view = Group('(' + Input.view + ')')
    from_sstream = Group('(' + Input.sstream + ')')
    from_stmt = FROM + (from_select("select") | from_module("module") | from_view("view") | from_sstream("sstream") | table_name_list("tables"))

    select_stmt <<= (SELECT + (column_name_list | '*')("columns") +
                     Optional(DISTINCT) +
                     Optional(from_stmt)("from") +
                     Optional(join_stmt) +
                     Optional(Group(WHERE + where_expression), "")("where") +
                     Optional(Group(union_select))('union') +
                     Optional(having))

    assign_select_stmt = (Combine(ident)("assign_var") + '=' + select_stmt).ignore(comment)

    def __init__(self):
        pass

    def debug(self):
        print(self.from_sstream.parseString("(SSTREAM @test)"))

    def parse_ternary(self, s):
        return self.ternary.parseString(s)

    def parse_if(self, s):
        return self.if_stmt.parseString(s)

    def parse_null_coal(self, s):
        return self.null_coal.parseString(s)

    def parse_cast(self, s):
        return self.cast_ident.parseString(s)

    def parse_aggr(self, s):
        return self.aggr_ident.parseString(s)

    def parse_join(self, s):
        return self.join_stmt.parseString(s)

    def parse_one_column(self, s):
        return self.one_column.parseString(s)

    def parse_select(self, s):
        return self.select_stmt.parseString(s)

    def parse_assign_select(self, s):
        return self.assign_select_stmt.parseString(s)

    def add_source(self, sources, parsed_result):
        if 'tables' in parsed_result:
            sources |= set(parsed_result['tables'])

        if 'table_name' in parsed_result:
            sources.add(parsed_result['table_name'])

        if 'join_table_name' in parsed_result:
            sources.add(parsed_result['join_table_name'])

        if 'union' in parsed_result:
            self.add_source(sources, parsed_result['union'][0])

        if 'module' in parsed_result:
            sources.add("MODULE_{}".format(parsed_result['module'][1]['module_dotname']))

        if 'view' in parsed_result:
            sources.add("VIEW_{}".format(parsed_result['view']))

        if 'sstream' in parsed_result:
            sources.add("SSTREAM_{}".format(parsed_result['sstream'][2]))

        if 'select' in parsed_result:
            self.add_source(sources, parsed_result['select'])

        return sources


    def parse(self, s):
        # specific output for our purpose
        ret = {
            'assign_var': None,
            'sources': set()
        }

        if s.strip().startswith('SELECT'):
            data = self.parse_select(s)
        else:
            # assign_select
            data = self.parse_assign_select(s)
            ret['assign_var'] = data['assign_var']

        self.add_source(ret['sources'], data)

#        print('-' *20)
#        print(json.dumps(data.asDict(), indent=4))
#        print('-' *20)
        return ret

if __name__ == '__main__':
    obj = Select()
    obj.debug()

    print(obj.parse_aggr('''SUM(CoImpressionCnt)'''))
    print(obj.parse_one_column('''SUM(CoImpressionCnt) AS CoImpressionCnt'''))
    print(obj.parse_one_column('''L.*'''))
    print(obj.parse_ternary('''YouOrderItemId == CompOrderItemId?"You" : Domain'''))
    print(obj.parse_ternary('''PagePosition.StartsWith("ML")?1:0 AS TopCnt,'''))
    print(obj.parse_null_coal('''AdId??0UL AS AdId'''))
    print(obj.parse_cast('''(byte?)MatchTypeId AS MatchTypeId'''))
    print(obj.parse_if('''IF(L.PositionNum < R.PositionNum, 0, 1)'''))

    print(obj.parse_select('''
        SELECT *
        FROM Step1
        UNION ALL
        SELECT *
        FROM ImpressionShare
    '''))

    print(obj.parse_select('''
        SELECT DateKey,
               HourNum,
               AccountId,
               SUM(ImpressionCntInAuction) AS ImpressionCntInAuction,
               SUM(TopCnt) AS TopCnt
        FROM Table1, Table2
    '''))

    print(obj.parse_select('''
        SELECT DateKey,
               DistributionChannelId,
               MediumId,
               DeviceTypeId,
    
               Domain,
               SUM(ImpressionCntInAuction) AS ImpressionCntInAuction,
               //SUM(CoImpression_AuctionLog) AS CoImpression_AuctionLog,    
               SUM(CoImpressionCnt) AS CoImpressionCnt
        FROM
        (
        SELECT *
        FROM Step1
        UNION ALL
        SELECT *
        FROM ImpressionShare
        )
        HAVING ImpressionCntInAuction + CoImpressionCnt + PositionNum + AboveCnt + TopCnt > 0;
    '''))

    print(obj.parse_select('''
            SELECT DateKey,
                   ListingId AS YouOrderItemId,
                   MBTimeBucket,
                   COUNT() AS AuctionCnt,
                   SUM(ImpressionCnt) AS ImpressionCnt
            FROM AuctionRaw;
    '''))

    print(obj.parse_select('''
            SELECT DateKey,
                   HourNum,
                   Domain,
                   //SUM(AuctionCnt) AS AuctionCnt,
                   SUM(ImpressionCnt) AS ImpressionCnt
            //SUM(CoImpressionCnt) AS CoImpressionCnt 
            FROM PairAggDomain
            HAVING ImpressionCnt > 0;
    '''))

    print(obj.parse_join('''
             INNER JOIN
                 AdDispayUrl AS R
             ON L.CompOrderItemId == R.ListingId && L.CompAdId == R.AdId;
    '''))


    print(obj.parse_select('''
        SELECT L.*,
               YouOrderItemId == CompOrderItemId?"You" : Domain AS Domain
        FROM RichPairAgg AS L
             INNER JOIN
                 AdDispayUrl AS R
             ON L.CompOrderItemId == R.ListingId && L.CompAdId == R.AdId;
    '''))

    print(obj.parse_select('''
        SELECT 
                CampaignTZDateKey AS DateKey,
                AdvertiserAccountId AS AccountId,
                AdId??0UL AS AdId,
                (byte?)MatchTypeId AS MatchTypeId,
                GeoLocationId,
                AbsPosition AS PositionNum,
                PagePosition.StartsWith("ML")?1:0 AS TopCnt
        FROM (
            MonetizationModules.MonetizationImpression(
                INPUT_BASE = @MonetizationCommonDataPath, 
                START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
                END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
            ))
        WHERE IsFraud == false && DupAdId == 0 && AdDisplayTypeId != 5 && MediumId IN (1,3) && LogDelta == @DateObj; 
    '''))

    print(obj.parse_select('''
        SELECT L.DateKey,
               L.HourNum,
               L.RGUID,
               R.PositionNum,
               IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
               R.TopCnt
        FROM AdImpressionRaw AS L
             INNER JOIN
                 AdImpressionRaw AS R
             ON L.RGUID == R.RGUID
        WHERE L.OrderItemId != R.OrderItemId;
    '''))

    print(obj.parse('''
        data = SELECT L.DateKey,
               R.PositionNum,
               IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
               R.TopCnt
        FROM LeftTable AS L
             INNER JOIN
                 RightTable AS R
             ON L.RGUID == R.RGUID
        WHERE L.OrderItemId != R.OrderItemId;
    '''))

    print(obj.parse('''
        SELECT L.DateKey,
               IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
               R.TopCnt
        FROM Table1, Table2
    '''))

    print(obj.parse('''
        SELECT L.DateKey,
               IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
               R.TopCnt
        FROM(
            MonetizationModules.MonetizationImpression(
                INPUT_BASE = @MonetizationCommonDataPath, 
                START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
                END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
            )        
        )
    '''))

    print(obj.parse('''
        AdImpressionRaw = SELECT 
                CampaignTZDateKey AS DateKey,
                CampaignTZHourNum AS HourNum,
                RGUID,
                AdvertiserAccountId AS AccountId,
                CampaignId,
                OrderId,
                OrderItemId,
                AdId??0UL AS AdId,
        
                (byte?)MatchTypeId AS MatchTypeId,
                RelationshipId,
                DistributionChannelId,
                MediumId,
                DeviceTypeId,
                GeoLocationId,
        
                AbsPosition AS PositionNum,
                PagePosition.StartsWith("ML")?1:0 AS TopCnt,
                ImpressionCnt
        FROM (
            MonetizationModules.MonetizationImpression(
                INPUT_BASE = @@MonetizationCommonDataPath@@, 
                START_DATETIME_UTC = @StartDateHourObj.AddHours(-2), 
                END_DATETIME_UTC=@StartDateHourObj.AddHours(2)
            ))
        WHERE IsFraud == false && DupAdId == 0 && AdDisplayTypeId != 5 && MediumId IN (1,3) LogDelta == @DateObj
    '''))

    print(obj.parse('''
        SELECT L.DateKey,
               L.ImpressionCnt AS YouImpressionCnt,
               L.PositionNum AS LPositionNum,
               L.RGUID,
               R.AccountId AS CompAccountId,
                     R.OrderItemId AS CompOrderItemId,
               R.AdId,
               IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt,
               R.TopCnt
        FROM AdImpressionRaw AS L
             INNER JOIN
                 AdImpressionRaw AS R
             ON L.RGUID == R.RGUID
        WHERE L.OrderItemId != R.OrderItemId
    '''))

    print(obj.parse('''
        RguidLevelAgg =
            SELECT DateKey,
                   HourNum,
                   GeoLocationId,
                   FIRST(YouImpressionCnt) AS YouImpressionCnt,
                   RGUID,
                   CompAccountId,
                   AdId,
                   SUM(AboveCnt) AS AboveCnt,
                   SUM(TopCnt) AS TopCnt    
    '''))

    print(obj.parse('''
        AdDispayUrl =
            SELECT ulong.Parse(AdId) AS AdId,
                   long.Parse(ListingId) AS ListingId,
                   Domain
            FROM
            (
                SSTREAM @FinalDomainPath
            )
    '''))

    print(obj.parse('''
        Listign2DomainAgg =
            SELECT DateKey,
                   Domain,
                   SUM(CoImpressionCnt) AS CoImpressionCnt,
                   SUM(TopCnt) AS TopCnt
            FROM Listing2Ad
                 INNER JOIN
                     AdDispayUrl
                 ON Listing2Ad.CompOrderItemId == AdDispayUrl.ListingId && Listing2Ad.AdId == AdDispayUrl.AdId
    '''))


    print(obj.parse('''
        SELECT *
        FROM Step1
        UNION ALL
        SELECT *
        FROM ImpressionShare
    '''))


    print(obj.parse('''
        YouPerfMerge =
            SELECT DateKey,
                   GeoLocationId,
                   SUM(AuctionCnt) AS AuctionCnt,
                   SUM(TopCnt) AS TopCnt
            FROM
            (
            SELECT *
            FROM YouPerfAuction
            UNION ALL
            SELECT *
            FROM YouPerfMonetization
            )
    '''))



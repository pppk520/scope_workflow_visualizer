from pyparsing import *
from common import Common


class Select(object):
    SELECT = Keyword("SELECT")
    FROM = Keyword("FROM")
    WHERE = Keyword("WHERE")
    JOIN = Keyword("JOIN")
    AS = Keyword("AS")
    UNION = Keyword("UNION")
    DISTINCT = Keyword("DISTINCT")

    cast = Group('(' + oneOf("long ulong short int byte") + ')')
    comment = Common.comment
    ident = Common.ident

    select_stmt = Forward()
    column_name = (delimitedList(ident, ".", combine=True)).setName("column name")
    cast_ident = Group(Optional(cast) + column_name).setName('cast_identifier')
    as_something = (AS + ident).setName('as_something')
    one_column = Group(cast_ident + Optional(as_something) | '*').setName('one_column')
    column_name_list = Group(delimitedList(one_column)).setName('column_name_list')
    table_name = (delimitedList(ident, ".", combine=True)).setName("table_name")
    table_name_list = Group(delimitedList(table_name))

    join = Group(Optional(oneOf('LEFT RIGHT OUTER')) + JOIN)
    union = Group(UNION + Optional('ALL'))

    where_expression = Forward()
    and_ = Keyword("AND")
    or_ = Keyword("OR")
    in_ = Keyword("IN")

    E = CaselessLiteral("E")
    binop = oneOf("== = != < > >= <=")
    arith_sign = Word("+-", exact=1)
    real_num = Combine(Optional(arith_sign) + (Word(nums) + "." + Optional(Word(nums)) |
                                               ("." + Word(nums))) +
                       Optional(E + Optional(arith_sign) + Word(nums)))
    int_num = Combine(Optional(arith_sign) + Word(nums) +
                      Optional(E + Optional("+") + Word(nums)))
    bool_val = oneOf("true false")

    column_rval = real_num | int_num | quotedString | column_name | bool_val # need to add support for alg expressions
    where_condition = Group(
        (column_name + binop + column_rval) |
        (column_name + in_ + "(" + delimitedList(column_rval) + ")") |
        (column_name + in_ + "(" + select_stmt + ")") |
        ("(" + where_expression + ")")
    )
    where_expression << where_condition + ZeroOrMore((and_ | or_) + where_expression)

    # define the grammar
    select_stmt <<= (SELECT + (column_name_list | '*')("columns") +
                     Optional(DISTINCT) +
                     FROM + table_name_list("tables") +
                     Optional(Group(WHERE + where_expression), "")("where"))

    union_select = ZeroOrMore(Optional(union) + select_stmt)

    select_stmt = select_stmt + union_select

    assign_select_stmt = (ident + '=' + select_stmt).ignore(comment)

    def __init__(self):
        pass

    def parse_select(self, s):
        return self.select_stmt.parseString(s)

    def parse_assign_select(self, s):
        return self.assign_select_stmt.parseString(s)

if __name__ == '__main__':
    s = '''
    SELECT *
    FROM Step1
    UNION ALL
    SELECT *
    FROM ImpressionShare
    '''

    s1 = '''
    Merge =
    SELECT DateKey,
           HourNum,
           AccountId,
           CampaignId,
           OrderId,
           YouOrderItemId,

           MatchTypeId,
           RelationshipId,
           DistributionChannelId,
           MediumId,
           DeviceTypeId,

           Domain,
           SUM(ImpressionCntInAuction) AS ImpressionCntInAuction,
           //SUM(CoImpression_AuctionLog) AS CoImpression_AuctionLog,    
           SUM(CoImpressionCnt) AS CoImpressionCnt,
           SUM(PositionNum) AS PositionNum,
           SUM(AboveCnt) AS AboveCnt,
           SUM(TopCnt) AS TopCnt
    FROM
    (
    SELECT *
    FROM Step1
    UNION ALL
    SELECT *
    FROM ImpressionShare
    )
    HAVING ImpressionCntInAuction + CoImpressionCnt + PositionNum + AboveCnt + TopCnt > 0; 
    '''

    parsed = Select().parse_select(s1)
    print(parsed)

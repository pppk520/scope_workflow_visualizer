from pyparsing import *
from scope_parser.common import Common
from scope_parser.input import Input
import json
import re


class Select(object):
    SELECT = Keyword("SELECT")
    FROM = Keyword("FROM")
    WHERE = Keyword("WHERE")
    JOIN = Keyword("JOIN") | Keyword("SEMIJOIN")
    CROSS_JOIN = Keyword("CROSS JOIN")
    CROSS_APPLY = Keyword("CROSS APPLY")
    AS = Keyword("AS")
    ON = Keyword("ON")
    UNION = Keyword("UNION")
    DISTINCT = Keyword("DISTINCT")
    HAVING = Keyword("HAVING")
    IF = Keyword("IF")

    comment = Common.comment
    ident = Common.ident
    ident_dot = Common.ident_dot
    value_str = Common.value_str
    func = Common.func
    func_chain = Common.func_chain
    func_chain_logical = delimitedList(func_chain, delim=oneOf("AND OR"))
    func_chain_not = Optional('!(') + (func_chain_logical | func_chain) + Optional(')')

    cast = Combine('(' + oneOf("long ulong short int byte double") + Optional('?') + ')')
    aggr = oneOf("SUM AVG MAX MIN COUNT FIRST")
    window_over_param = 'PARTITION BY' + delimitedList(ident)

    E = CaselessLiteral("E")
    binop = oneOf("== = != < > >= <=")
    arith_sign = Word("+-", exact=1)

    # YouOrderItemId == CompOrderItemId?"You" : Domain AS Domain
    extend_ident = delimitedList(ident, delim='.', combine=True)
    ternary_condition_binop = Group(extend_ident + binop + (extend_ident | value_str))
    ternary_condition_func = func_chain
    ternary_val = extend_ident | value_str
    ternary = Optional('(') + (ternary_condition_binop | ternary_condition_func) + '?' + ternary_val + ':' + ternary_val + Optional(')')

    # AdId??0UL AS AdId
    null_coal = ident_dot + '??' + value_str

    # IF(L.PositionNum < R.PositionNum, 0, 1) AS AboveCnt
    # IF(DailyBudgetUSD == null || MPISpend/100.0 <= DailyBudgetUSD, 1.0, DailyBudgetUSD/(MPISpend/100.0)) AS BudgetFactor
    #if_stmt = Group(IF + '(' + (ternary_condition_binop | ternary_condition_func) + ',' + value_str + ',' + value_str + ')')
    if_stmt_base = Group(IF + Regex('\(.*\)', re.DOTALL))
    if_stmt_as = Group(IF + Regex('\(.*\) AS ([^,^ ]+)', re.DOTALL))

    if_stmt = Optional(cast) + (if_stmt_as | if_stmt_base)

    and_ = Keyword("AND")
    or_ = Keyword("OR")
    in_ = Keyword("IN")

    select_stmt = Forward()
    column_name = Group(delimitedList(ident | '*', "."))

    real_num = Group(Optional(arith_sign) + (Word(nums) + "." + Optional(Word(nums)) |
                                               ("." + Word(nums))) +
                     Optional(E + Optional(arith_sign) + Word(nums)))
    int_num = Group(Optional(arith_sign) + Word(nums) +
                      Optional(E + Optional("+") + Word(nums)))
    bool_val = oneOf("true false")

    column_rval = real_num | int_num | quotedString | column_name | bool_val # need to add support for alg expressions

    cast_ident = Group(Optional(cast) + column_name).setName('cast_identifier')
    cast_num = Group(Optional(cast) + column_rval).setName('cast_num')
    operator_ident = Group(ident + OneOrMore(oneOf('+ - * /') + ident))
    aggr_ident_basic = Group(aggr + '(' + (operator_ident | ident | Empty()) + ')')
    aggr_ident_operator = aggr_ident_basic + ZeroOrMore(oneOf('+ - * /') + (aggr_ident_basic | ident))
    aggr_ident_over = aggr_ident_basic + 'OVER' + '(' + window_over_param + ')'
    aggr_ident = Optional(cast) + (aggr_ident_over | aggr_ident_operator)
    distinct_ident = DISTINCT + ident
    new_something = 'new' + func_chain;
    as_something = (AS + ident).setName('as_something')

    one_column = Group((distinct_ident |
                        aggr_ident |
                        ternary |
                        null_coal |
                        if_stmt |
                        new_something |
                        func_chain_not |
                        operator_ident |
                        cast_ident |
                        cast_num |
                        quotedString)('column_name') + Optional(as_something) | '*').setName('one_column')

    column_name_list = Group(delimitedList(one_column))('column_name_list')
    table_name = (delimitedList(ident, ".", combine=True))("table_name")
    table_name_list = delimitedList(table_name + Optional(as_something).suppress()) # AS something, don't care
    bond_expr = Combine(func + ZeroOrMore('.' + ident))

    union = Group(UNION + Optional('ALL' | DISTINCT))

    where_expression = Forward()

    where_condition = Group(
        (column_name + binop + Group(column_rval)) |
        (column_name + in_ + Group("(" + delimitedList(Combine(column_rval)) + ")")) |
        (column_name + in_ + Group("(" + select_stmt + ")")) |
        ("(" + where_expression + ")")
    )
    where_expression << where_condition + ZeroOrMore((and_ | or_ | '&&' | '|') + where_expression)

    join = Group(Optional(OneOrMore(oneOf('LEFT RIGHT OUTER INNER'))) + JOIN)
    join_stmt = join + table_name("join_table_name*") + Optional(AS + ident) + ON + where_expression
    cross_join_stmt = CROSS_JOIN + table_name("join_table_name*")
    cross_apply_stmt = CROSS_APPLY + (bond_expr("bond") | table_name) + Optional(AS + ident)

    # define the grammar
    union_select = ZeroOrMore(Optional(union) + select_stmt)
    having = HAVING + restOfLine

    from_select = Group('(' + select_stmt + ')' + Optional(as_something).suppress())
    from_module = Group('(' + Input.module + ')' + Optional(as_something).suppress())
    from_view = Group('(' + Input.view + ')' + Optional(as_something).suppress())
    from_sstream = Group('(' + Input.sstream + ')' + Optional(as_something).suppress())
    from_stmt = FROM + (from_select("select") | from_module("module") | from_view("view") | from_sstream("sstream") | table_name_list("tables"))

    select_stmt <<= (SELECT + (column_name_list | '*')("columns") +
                     Optional(DISTINCT) +
                     Optional(from_stmt)("from") +
                     ZeroOrMore(join_stmt | cross_join_stmt) +
                     Optional(cross_apply_stmt) +
                     Optional(Group(WHERE + where_expression))("where") +
                     Optional(Group(union_select))('union') +
                     Optional(having))

    select_union = select_stmt + ZeroOrMore(union + select_stmt)

    assign_select_stmt = (Combine(ident)("assign_var") + '=' + select_union).ignore(comment)

    def __init__(self):
        pass

    def debug(self):
        print(self.where_expression.parseString('CustomerId != - 1'))


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
            for table_name in parsed_result['join_table_name']:
                sources.add(table_name)

        if 'union' in parsed_result:
            self.add_source(sources, parsed_result['union'][0])

        if 'module' in parsed_result:
            sources.add("MODULE_{}".format(parsed_result['module'][1]['module_dotname']))

        if 'view' in parsed_result:
            sources.add("VIEW_{}".format(parsed_result['view']['from_source']))

        if 'sstream' in parsed_result:
            sources.add("SSTREAM_{}".format(parsed_result['sstream'][2]))

        if 'select' in parsed_result:
            self.add_source(sources, parsed_result['select'])

        if 'bond' in parsed_result:
            sources.add("BOND_{}".format(parsed_result['bond']))

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

    print(obj.parse('''
RejectionRule = 
    SELECT CustomerId,
           AccountId
    FROM (VIEW @RejectRuleView)
    WHERE CustomizationConditions.Contains("Deduped:Yes") AND TacticCode IN("IN1-I", "BMM-I", "SM1-I", "BMA-I", "SKW-I"); 
                    '''))



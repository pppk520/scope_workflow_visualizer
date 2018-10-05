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
    IMPORT = Keyword("IMPORT")

    extract_data_type = oneOf("string int ulong long short byte decimal double DateTime")

    ##################################
    # General
    ##################################
    ident = Common.ident
    comment = Common.comment
    func = Common.func
    func_chain = Common.func_chain
    func_ptr = Common.func_ptr
    func_param = Common.func_params
    value_str = Common.value_str

    value_pattern = Combine(Optional('@') + quotedString)

    param_assign = ident + '=' + Combine(Optional('@') + (func_chain | func_ptr | value_str))('param_value*')
    param_assign_list = delimitedList(param_assign)
    params = PARAMS + '(' + param_assign_list + ')'
    dot_name = delimitedList(ident, delim='.', combine=True)

    extract_column = Group(ident + Optional(':' + extract_data_type + Optional('?')))

    ##################################
    # Different Input Categories
    ##################################
    parentheses_func_param = '(' + func_param + ')'
    streamset_type = oneOf("__datetime __hour __day __year __serialnum")
    list_values = delimitedList(value_str)
    streamset_range = streamset_type + '=' + Group('[' + list_values + ']' + Optional(parentheses_func_param))

    streamset = STREAMSET + value_str('from_source') + PATTERN + value_pattern + RANGE + streamset_range
    sstream_value_streamset = SSTREAM + streamset
    sstream = SSTREAM + value_str('from_source')
    extract = EXTRACT + delimitedList(extract_column) + FROM + value_str('from_source') + Optional(USING + func)
    view = VIEW + Combine(value_str)('from_source') + Optional(params)
    module = Group(dot_name("module_dotname") + '(' + param_assign_list + ')')

    assign_sstream = Combine(ident)('assign_var') + '=' + sstream('sstream')
    assign_sstream_streamset = Combine(ident)('assign_var') + '=' + sstream_value_streamset('sstream_streamset')
    assign_extract = Combine(ident)('assign_var') + '=' + extract('extract')
    assign_module = Combine(ident)('assign_var') + '=' + module('module')
    assign_view = Combine(ident)('assign_var') + '=' + view('view')
    import_as = IMPORT + quotedString('from_source') + "AS" + Combine(ident)('assign_var') + Optional(params)

    def debug(self):
        print(self.param_assign_list.parseString('''
        KeywordAdvertiser = OrderSuggKW_CrossLCID,
        CampaignLocationTarget = NKWData.CampaignLocationTarget,
        LowTrafficKw = NKWData.LowTrafficKw,
        LowTrafficAdGroup = NKWData.LowTrafficAdGroup
        '''))

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
            if 'STREAMSET' in s:
                ret = self.assign_sstream_streamset.parseString(s)
                d['sources'].add('SSTREAM_' + ret['from_source'])
            else:
                ret = self.assign_sstream.parseString(s)
                d['sources'].add('SSTREAM_' + ret['from_source'])
        elif 'VIEW' in s:
            ret = self.assign_view.parseString(s)
            d['sources'].add('VIEW_' + ret['from_source'])
        elif 'IMPORT' in s:
            ret = self.import_as.parseString(s)
            d['sources'].add('IMPORT_' + ret['from_source'])
        else:
            # try module parsing
            ret = self.assign_module.parseString(s)
            if ret and 'module' in ret:
                d['sources'].add('MODULE_' + ret['module']['module_dotname'])

                if 'param_value' in ret['module']:
                    d['params'] = set()

                    for param_value in ret['module']['param_value']:
                        d['params'].add(param_value)

            else:
                raise NotImplementedError('unsupported input type?')

        d['assign_var'] = ret['assign_var']

        return d


if __name__ == '__main__':
    i = Input()
    i.debug()

    print(i.parse('''
HealthCheckAdoption_OneEntity = 
    EXTRACT AdoptionDateTime : DateTime,
            AccountId : int,
            EntityId : long,
            CheckPoint : string
    FROM @EmptyFilePath
    USING DefaultTextExtractor("-d", ",")
'''))


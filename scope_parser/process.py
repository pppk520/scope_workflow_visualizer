from pyparsing import *
from scope_parser.common import Common

class Process(object):
    PROCESS = Keyword("PROCESS")
    PRODUCE = Keyword("PRODUCE")
    USING = Keyword("USING")
    HAVING = Keyword("HAVING")

    ident = Common.ident
    produce_schema = delimitedList(ident)
    func = Common.func
    func_ptr = Common.func_ptr

    using_func = USING + (func | func_ptr)('using')
    process_implicit = PROCESS + Optional(PRODUCE + produce_schema) + using_func
    process_explicit = PROCESS + Combine(ident)('source') + Optional(PRODUCE + produce_schema) + using_func
    process_select = PROCESS + '(' + Regex('[^\(\)]+') + ')' + using_func
    process_stmt = process_explicit | process_implicit | process_select

    assign_process_stmt = Combine(ident)('assign_var') + '=' + process_stmt

    process_both = assign_process_stmt | process_stmt

    def parse(self, s):
        ret = {
            'assign_var': None,
            'sources': set(),
            'using': None
        }

        d = self.process_both.parseString(s)

        ret['assign_var'] = d.get('assign_var', None)
        if 'source' in d:
            ret['sources'].add(d['source'])

        ret['using'] = d['using'][0]

        return ret

if __name__ == '__main__':
    obj = Process()

    print(obj.parse('''
SMT =
    PROCESS
    (
        SELECT Keyword,
               ListingId,
               Title,
               URL,
               AdText
        FROM DECorpus
    )
    USING GenericExeInvokingProcessor("Bing1Year_GI_IDF_CocountGt300_4Relevane_Unigram_TM_2Input.zip", "Append", "SMT_KT,SMT_TK,SMT_KU,SMT_UK,SMT_KC,SMT_CK", "InputGeneratorFromRowSet", "AllRowsExceptHeaderParser", "0,1,2", "", "run.cmd", "Bing1Year_GI_IDF_CocountGt300_4Relevane_Unigram_IdHash_dat.tsv", "Bing1Year_GI_IDF_CocountGt300_4Relevane_Unigram_TM1_dat.tsv", "Bing1Year_GI_IDF_CocountGt300_4Relevane_Unigram_TM2_dat.tsv", "##INPUT##", "##OUTPUT##", "1e-20", "0.5", "1", "Keyword", "Title:URL:AdText", "logp_qd_:logp_dq_")

        '''))




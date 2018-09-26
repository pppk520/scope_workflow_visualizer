import re
from pyparsing import *
from scope_parser.common import Common

class Declare(object):
    DECLARE = Keyword("#DECLARE")
    DATA_TYPE = oneOf("string String DateTime int bool double long ulong float")('data_type')

    data_type_map = 'MAP' + '<' + DATA_TYPE + ',' + DATA_TYPE + Optional('?') + '>'

    ident = Common.ident

    declare = DECLARE + Combine(ident)('key') + (DATA_TYPE | data_type_map) + '=' + Regex('(.*?);', flags=(re.DOTALL|re.MULTILINE))

    def parse(self, s):
        s = s + ';' # ; is our end of string, must be existing
        data = self.declare.searchString(s)

        if data:
            return data[0][1].strip(), \
                   data[0][-1].replace('\n', '').rstrip(';').strip()

        return None, None

    def debug(self):
        s = 'string.Format(@"{0}/Preparations/MPIProcessing/{1:yyyy/MM/dd}/Campaign_TargetInfo_{1:yyyyMMdd}.ss", @KWRawPath,  DateTime.Parse(@RunDate))'
        print(s[121 - 5: 121 + 5])
        print(self.conversion_stmt.parseString(s))

if __name__ == '__main__':
    d = Declare()
    #d.debug()

    print(d.parse('''
    #DECLARE MeanOfCTR float = 0.1f
        '''))

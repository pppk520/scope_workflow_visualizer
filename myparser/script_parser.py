import re
import logging

from util.file_utility import FileUtility

from scope_parser.declare import Declare
from scope_parser.input import Input
from scope_parser.output import Output
from scope_parser.module import Module
from scope_parser.process import Process
from scope_parser.using import Using
from scope_parser.select import Select

class ScriptParser(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.vars = {}

        self.declare = Declare()
        self.input = Input()
        self.output = Output()
        self.module = Module()
        self.process = Process()
        self.using = Using()
        self.select = Select()

    def remove_empty_lines(self, content):
        return "\n".join([ll.rstrip() for ll in content.splitlines() if ll.strip()])

    def remove_comments(self, content):
        re_comment = re.compile(r'/\*(.*?)\*/', re.MULTILINE | re.DOTALL)

        # remove comments
        content = re.sub(re_comment, '', content)
        content = re.sub(r'(//.*)\n', '\n', content)

        return content

    def remove_if(self, content):
        re_if = re.compile(r'#IF.*?\n(.*?)#ENDIF', re.MULTILINE | re.DOTALL)

        content = re.sub(re_if, '\g<1>', content)

        return content

    def resolve_external_params(self, content, params={}):
        self.logger.debug('params = {}'.format(params))

        re_external_param = re.compile(r'@@(.*?)@@')

        def replace_matched(match):
            text = match.group()
            return params.get(match.group(1), text)

        return re_external_param.sub(replace_matched, content)

    def parse_file(self, filepath, external_params={}):
        content = FileUtility.get_file_content(filepath)
#        content = self.remove_empty_lines(content)
        content = self.remove_comments(content)
        content = self.remove_if(content)
        content = self.resolve_external_params(content, external_params)

        parts = content.split(';')

        declare_map = {}

        for part in parts:
            print('-' * 20)
            print(part)

            if '#DECLARE' in part:
                key, value = self.declare.parse(part)
                declare_map[key] = value

                print('declare [{}] as [{}]'.format(key, value))
            elif 'SELECT' in part:
                d = self.select.parse(part)
                print(d)

        print(declare_map)

if __name__ == '__main__':
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\SOV\Scope\AuctionInsight\scripts\AucIns_Final.script''')
#    print(ScriptParser().resolve_external_params(s, {'external': 'yoyo'}))
#    print(ScriptParser().resolve_declare(s_declare))
#    ScriptParser().parse_file('../tests/files/SOV3_StripeOutput.script')

import re
import logging
import json

from util.file_utility import FileUtility

from scope_parser.declare import Declare
from scope_parser.input import Input
from scope_parser.output import Output
from scope_parser.module import Module
from scope_parser.process import Process
from scope_parser.using import Using
from scope_parser.select import Select

from graph.node import Node
from graph.edge import Edge
from graph.graph_utility import GraphUtility

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

    def find_latest_node(self, target_name, nodes):
        for node in nodes[::-1]:
            if node.name == target_name:
                return node

        self.logger.warning('cannot find node [{}]! Probably source node.'.format(target_name))

        return Node(target_name)

    def parse_file(self, filepath, external_params={}, dest_filepath=None):
        content = FileUtility.get_file_content(filepath)
#        content = self.remove_empty_lines(content)
        content = self.remove_comments(content)
        content = self.remove_if(content)
        content = self.resolve_external_params(content, external_params)

        parts = content.split(';')

        declare_map = {}

        nodes = []
        edges = []
        all_nodes = [] # add node to networkx ourself, missing nodes in edges will be added automatically
                       # and the id of auto-added nodes are not controllable

        for part in parts:
            self.logger.debug('-' * 20)
            self.logger.debug(part)

            if '#DECLARE' in part:
                key, value = self.declare.parse(part)
                declare_map[key] = value

                self.logger.info('declare [{}] as [{}]'.format(key, value))
            elif 'SELECT' in part:
                d = self.select.parse(part)
                self.logger.debug(d)
                self.logger.info('[{}] = select from sources [{}]'.format(d['assign_var'], d['sources']))

                if d['assign_var']:
                    nodes.append(Node(d['assign_var']))
                    all_nodes.append(nodes[-1])

                to_node = nodes[-1]

                if len(d['sources']) == 0:
                    from_node = nodes[-2]
                    edges.append(Edge(from_node, to_node))
                else:
                    for source in d['sources']:
                        from_node = self.find_latest_node(source, nodes)
                        all_nodes.append(from_node)
                        edges.append(Edge(from_node, to_node))

            elif 'OUTPUT' in part:
                d = self.output.parse(part)
                self.logger.debug(d)

                if not d['ident']:
                    d['ident'] = nodes[-1].name

                from_node = self.find_latest_node(d['ident'], nodes)
                to_node = Node(d['path'], attr={'type': 'output'})
                all_nodes.append(to_node)

                edges.append(Edge(from_node, to_node))

        self.logger.info(declare_map)

        if dest_filepath:
            self.logger.info('output GEXF to [{}]'.format(dest_filepath))
            GraphUtility().to_gexf_file(all_nodes, edges, dest_filepath)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

#    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\SOV\Scope\AuctionInsight\scripts\AucIns_Final.script''', dest_filepath='d:/tmp/tt.gexf')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/6.MPIProcessing.script''', dest_filepath='d:/tmp/tt.gexf')
#    print(ScriptParser().resolve_external_params(s, {'external': 'yoyo'}))
#    print(ScriptParser().resolve_declare(s_declare))
#    ScriptParser().parse_file('../tests/files/SOV3_StripeOutput.script')

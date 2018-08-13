import re
import logging
import json
import os
import configparser

from util.file_utility import FileUtility

from scope_parser.declare import Declare
from scope_parser.set import Set
from scope_parser.input import Input
from scope_parser.output import Output
from scope_parser.module import Module
from scope_parser.process import Process
from scope_parser.reduce import Reduce
from scope_parser.using import Using
from scope_parser.select import Select
from myparser.scope_resolver import ScopeResolver

from graph.node import Node
from graph.edge import Edge
from graph.graph_utility import GraphUtility

class ScriptParser(object):
    logger = logging.getLogger(__name__)

    def __init__(self, external_params={}, b_add_sstream_link=True):
        self.vars = {}

        self.declare = Declare()
        self.set = Set()
        self.input = Input()
        self.output = Output()
        self.module = Module()
        self.process = Process()
        self.reduce = Reduce()
        self.using = Using()
        self.select = Select()

        self.scope_resolver = ScopeResolver()

        self.b_add_sstream_link = b_add_sstream_link
        self.sstream_link_prefix = ""
        self.sstream_link_suffix = ""
        self.external_params = {}

        # read config from ini file
        config_filepath = os.path.join(os.path.dirname(__file__), os.pardir, 'config', 'config.ini')
        self.read_configs(config_filepath)

        # overwrite external params
        self.external_params.update(external_params)

    def read_configs(self, filepath):
        config = configparser.ConfigParser()
        config.optionxform = str # reserve case
        config.read(filepath)

        self.sstream_link_prefix = config['ScriptParser']['sstream_link_prefix']
        self.sstream_link_suffix = config['ScriptParser']['sstream_link_suffix']

        for key in config['ExternalParam']:
            self.external_params[key] = config['ExternalParam'][key]

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

    def upsert_node(self, node_map, node_name):
        if node_name not in node_map:
            self.logger.info('cannot find node [{}]! Probably source node.'.format(node_name))

            node_map[node_name] = Node(node_name)

    def remove_loop(self, content):
        ''' Assumption: end bracelet '}' of LOOP is isolated in single line
        If not, should use stack to process char by char

        :param content: the scope script body
        :return: cleaned content
        '''
        lines = content.splitlines()

        result_lines = []

        loop_on = False
        for line in lines:
            if loop_on:
                if line.strip() == '}':
                    loop_on = False
                    continue
                elif line.strip() == '{':
                    continue

            if 'LOOP' in line:
                loop_on = True
                continue

            result_lines.append(line)

        return '\n'.join(result_lines)

    def remove_data_hint(self, content):
        re_dh = re.compile(r'^\[.*?\]', re.MULTILINE | re.DOTALL)
        content = re.sub(re_dh, '', content)

        return content

    def add_sstream_link(self, nodes, declare_map):
        for node in nodes:
            # only target SSTREAM
            if not node.name.startswith('SSTREAM_'):
                continue

            param = node.name[node.name.index('_') + 1:]

            body_str = declare_map[param]
            # remove date or streamset query
            if isinstance(body_str, str) and '?' in body_str:
                self.logger.info('remove query string of [{}]'.format(body_str))
                body_str = body_str[:body_str.index('?')]

            href = '{}{}{}'.format(self.sstream_link_prefix,
                                   body_str,
                                   self.sstream_link_suffix)

            # change node label to html format for different font size
            label = '<{} <BR/> <FONT POINT-SIZE="4">{}</FONT>>'.format(node.attr['label'], href)

            node.attr['label'] = label
            node.attr['href'] = href # not work when rendered to pdf, works in jupyter

    def change_node_color(self, nodes):
        for node in nodes:
            if '_' not in node.name:
                continue

            input_type = node.name.split('_')[0]

            if input_type not in ['SSTREAM', 'EXTRACT', 'MODULE', 'VIEW', 'BOND']:
                continue

            attr = {}
            attr.update({'type': 'input',
                         'style': 'filled'})

            # color scheme: https://www.graphviz.org/doc/info/colors.html#brewer
            if input_type == 'SSTREAM':
                attr['fillcolor'] = 'greenyellow'
            elif input_type == 'EXTRACT':
                attr['fillcolor'] = 'honeydew'
            elif input_type == 'MODULE':
                attr['fillcolor'] = 'sandybrown'
            elif input_type == 'VIEW':
                attr['fillcolor'] = 'lightpink'
            elif input_type == 'BOND':
                attr['fillcolor'] = 'lightblue'

            node.attr.update(attr)

    def process_output(self, part, node_map, all_nodes, edges):
        d = self.output.parse(part)
        self.logger.debug(d)

        source_name = d['ident']
        from_node = node_map.get(source_name, node_map['last_node'])
        to_node = Node(d['path'], attr={'type': 'output',
                                        'style': 'filled',
                                        'fillcolor': 'tomato'})

        edges.append(Edge(from_node, to_node))
        all_nodes.append(to_node)

    def process_extract(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_view(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_input_sstream(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_input_module(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_process(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.process.parse(part))

    def process_reduce(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.reduce.parse(part))

    def process_select(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.select.parse(part))

    def connect_module_params(self, node_map, all_nodes, edges, dest_node, params):
        for param in params:
            if not param in node_map:
                # do nothing if param not appeared before
                continue

            param_node = node_map[param]
            edges.append(Edge(param_node, dest_node))

    def process_core(self, part, node_map, all_nodes, edges, d):
        from_nodes = []
        to_node = None

        for source in d['sources']:
            self.upsert_node(node_map, source)  # first, check and upsert if not in node_map
            from_nodes.append(node_map[source])

            if source.startswith('MODULE_'):
                self.connect_module_params(node_map, all_nodes, edges, node_map[source], d.get('params', []))

        if len(from_nodes) == 0:
            from_nodes.append(node_map['last_node'])

        if d['assign_var']:
            node_name = d['assign_var']
            new_node = Node(node_name)
            node_map[node_name] = new_node  # update
            to_node = new_node
        else:
            to_node = node_map['last_node']

        for from_node in from_nodes:
            edges.append(Edge(from_node, to_node))
            all_nodes.append(from_node)

        all_nodes.append(to_node)
        node_map['last_node'] = to_node

    def process_declare(self, part, declare_map):
        key, value = self.declare.parse(part)
        declare_map['@' + key] = value

        self.logger.info('declare [{}] as [{}]'.format(key, value))

    def process_set(self, part, declare_map):
        key, value = self.set.parse(part)

        if 'IF' in value:
            self.logger.info('for now, we do not handle IF statement.')
            return

        declare_map['@' + key] = value

        self.logger.info('set [{}] as [{}]'.format(key, value))

    def parse_file(self, filepath, external_params={}, dest_filepath=None):
        self.external_params.update(external_params)

        content = FileUtility.get_file_content(filepath)
#        content = self.remove_empty_lines(content)
        content = self.remove_comments(content)
        content = self.remove_if(content)
        content = self.resolve_external_params(content, self.external_params)
        content = self.remove_loop(content)
        content = self.remove_data_hint(content)

        parts = content.split(';')

        declare_map = {}

        node_map = {'last_node': None}
        edges = []
        all_nodes = [] # add node to networkx ourself, missing nodes in edges will be added automatically
                       # and the id of auto-added nodes are not controllable

        for part in parts:
            self.logger.debug('-' * 20)
            self.logger.debug(part)

            # ignore data after C# block
            if '#CS' in part:
                self.logger.info('meet CS block, break parsing.')
                break

            if '#DECLARE' in part:
                # some files contain prefix unicode string
                self.process_declare(part, declare_map)
            elif '#SET' in part:
                self.process_set(part, declare_map)
            elif 'SELECT' in part:
                self.process_select(part, node_map, all_nodes, edges)
            elif 'SSTREAM' in part and not 'OUTPUT' in part:
                self.process_input_sstream(part, node_map, all_nodes, edges)
            elif 'EXTRACT' in part:
                self.process_extract(part, node_map, all_nodes, edges)
            elif 'VIEW' in part:
                self.process_view(part, node_map, all_nodes, edges)
            elif 'PROCESS' in part:
                self.process_process(part, node_map, all_nodes, edges)
            elif 'REDUCE' in part:
                self.process_reduce(part, node_map, all_nodes, edges)
            elif 'OUTPUT' in part:
                self.process_output(part, node_map, all_nodes, edges)
            else:
                try:
                    self.process_input_module(part, node_map, all_nodes, edges)
                except Exception as ex:
                    # do nothing if parsing module failed, probably it's not from module
                    pass

        self.logger.info(declare_map)

        self.change_node_color(all_nodes)
        self.scope_resolver.resolve_declare(declare_map)

        if self.b_add_sstream_link:
            self.add_sstream_link(all_nodes, declare_map)

        if dest_filepath:
            self.logger.info('change node color for output')

            gu = GraphUtility(all_nodes, edges)

            gexf_output_file = gu.to_gexf_file(dest_filepath)
            self.logger.info('output .gexf file to [{}]'.format(gexf_output_file))

            dot_output_file = gu.to_dot_file(dest_filepath)
            self.logger.info('output .dot file to [{}]'.format(dot_output_file))

            self.logger.info('render graphviz file')
            gu.dot_to_graphviz(dot_output_file)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\SOV\Scope\AuctionInsight\scripts\AucIns_Final.script''', dest_filepath='d:/tmp/AucIns_Final.script')
    ScriptParser().parse_file(
        '''D:\workspace\AdInsights\private\Backend\SOV\Scope\ImpressionShare\ImpressionSharePipeline\scripts\SOV3_StripeOutput.script''',
        dest_filepath='d:/tmp/SOV3_StripeOutput.script')
    ScriptParser().parse_file('''D:/workspace/AdInsights/private/Backend/UCM/Src/Scope/UCM_CopyTaxonomyVertical.script''', dest_filepath='d:/tmp/UCM_CopyTaxonomyVertical.script')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/1.MergeSources.script''', dest_filepath='d:/tmp/1.MergeSources.script')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/6.MPIProcessing.script''', dest_filepath='d:/tmp/6.MPIProcessing.script')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/7.PKVGeneration_BMMO.script''', dest_filepath='d:/tmp/7.PKVGeneration_BMMO.script')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/7.PKVGeneration_BMO.script''', dest_filepath='d:/tmp/7.PKVGeneration_BMO.script')
    ScriptParser().parse_file('''D:\workspace\AdInsights\private\Backend\Opportunities\Scope\KeywordOpportunitiesV2\KeywordOpportunitiesV2/7.PKVGeneration_KWO.script''', dest_filepath='d:/tmp/7.PKVGeneration_KWO.script')

#    print(ScriptParser().resolve_external_params(s, {'external': 'yoyo'}))
#    print(ScriptParser().resolve_declare(s_declare))
#    ScriptParser().parse_file('../tests/files/SOV3_StripeOutput.script')

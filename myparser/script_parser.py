import re
import logging
import json
import os
import configparser

from util.file_utility import FileUtility
from util.datetime_utility import DatetimeUtility

from scope_parser.declare import Declare
from scope_parser.set import Set
from scope_parser.input import Input
from scope_parser.output import Output
from scope_parser.module import Module
from scope_parser.process import Process
from scope_parser.reduce import Reduce
from scope_parser.combine import Combine
from scope_parser.using import Using
from scope_parser.select import Select
from scope_parser.loop import Loop
from myparser.scope_resolver import ScopeResolver

from graph.node import Node
from graph.edge import Edge
from graph.graph_utility import GraphUtility
from cosmos.sstream_utiltiy import SstreamUtility

class ScriptParser(object):
    logger = logging.getLogger(__name__)

    def __init__(self, b_add_sstream_link=True, b_add_sstream_size=True):
        self.vars = {}

        self.declare = Declare()
        self.set = Set()
        self.input = Input()
        self.output = Output()
        self.module = Module()
        self.process = Process()
        self.reduce = Reduce()
        self.combine = Combine()
        self.using = Using()
        self.select = Select()

        self.scope_resolver = ScopeResolver()

        self.b_add_sstream_link = b_add_sstream_link
        self.b_add_sstream_size = b_add_sstream_size
        self.sstream_link_prefix = ""
        self.sstream_link_suffix = ""
        self.external_params = {}

        # read config from ini file
        config_filepath = os.path.join(os.path.dirname(__file__), os.pardir, 'config', 'config.ini')
        self.read_configs(config_filepath)

        self.ssu = SstreamUtility("d:/workspace/dummydummy.ini") # specify your auth file path

    def read_configs(self, filepath):
        config = configparser.ConfigParser()
        config.optionxform = str # reserve case
        config.read(filepath)

        self.sstream_link_prefix = config['ScriptParser']['sstream_link_prefix']
        self.sstream_link_suffix = config['ScriptParser']['sstream_link_suffix']

        for key in config['ExternalParam']:
            if key.startswith('#'):
                continue

            self.external_params[key] = config['ExternalParam'][key]

        # make it default to 5 days ago
        default_date_str = DatetimeUtility.get_datetime_str(delta_days=-5, fmt_str='%Y-%m-%d')
        if 'RunDate' not in self.external_params:
            self.external_params['RunDate'] = default_date_str
        if 'Date' not in self.external_params:
            self.external_params['Date'] = default_date_str
        if 'PROCESS_DATE' not in self.external_params:
            self.external_params['PROCESS_DATE'] = default_date_str


    def remove_empty_lines(self, content):
        return "\n".join([ll.rstrip() for ll in content.splitlines() if ll.strip()])

    def remove_comments(self, content):
        # handy function from https://stackoverflow.com/questions/241327/python-snippet-to-remove-c-and-c-comments
        def replacer(match):
            s = match.group(0)
            if s.startswith('/'):
                return " "  # note: a space and not an empty string
            else:
                return s

        pattern = re.compile(
            r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
            re.DOTALL | re.MULTILINE
        )

        return re.sub(pattern, replacer, content)

    def remove_if(self, content, keep_if_content=True):
        re_if = re.compile(r'#IF.*?\n(.*?)#ENDIF', re.MULTILINE | re.DOTALL)

        if keep_if_content:
            content = re.sub(re_if, '\g<1>', content)
        else:
            content = re.sub(re_if, '', content)

        # remove inner '#ELSE'
        # which means #IF (block_1) #ELSE (block_2) #ENDIF
        # after substitution it will be {block_1} {block_2}
        content = re.sub(r'#ELSE', '', content)

        return content


    def resolve_external_params(self, content, params={}):
        self.logger.debug('params = {}'.format(params))

        re_external_param = re.compile(r'@@(.*?)@@')

        def replace_matched(match):
            text = match.group()
            return params.get(match.group(1), text).replace('""', '"')

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

    def expand_loop(self, content):
        ''' Assumption: end bracelet '}' of LOOP is isolated in single line
        If not, should use stack to process char by char

        Note: LOOP is officially unrecommended operation
        https://stackoverflow.microsoft.com/questions/5174/where-can-i-find-more-information-about-the-scope-keyword-loop/5175#5175

        :param content: the scope script body
        :return: expanded content
        '''
        lines = content.splitlines()

        result_lines = []

        loop_on = False
        var = None
        loop_count = 0
        loop_content = []

        for line in lines:
            if loop_on:
                # string.Format has format placeholder {}
                if '}' in line.strip() and not 'string.format' in line.lower():
                    loop_on = False

                    # it can also be param in declare_map, just ignore this case now
                    if self.is_int(loop_count):
                        for i in range(int(loop_count)):
                            for content_line in loop_content:
                                result_lines.append(content_line.replace('@@{}@@'.format(var), str(i)))
                    else:
                        result_lines.extend(loop_content)

                    # anything after enclosing should be kept
                    result_lines.append(line.replace('}', ''))

                    continue
                elif line.strip() == '{':
                    continue

                loop_content.append(line)
                continue

            if 'LOOP' in line:
                var, loop_count = Loop().get_var_loop_count(line)
                self.logger.debug('found keyword LOOP, var = {}, loop_count = {}'.format(var, loop_count))

                if var is not None and loop_count is not None:
                    loop_on = True
                    loop_content = []
                    continue

            result_lines.append(line)

        return '\n'.join(result_lines)

    def remove_data_hint(self, content):
        #[ROWCOUNT=100]
        re_dh_1 = re.compile(r'\[.+?=[ ]?\d+?\]')
        content = re.sub(re_dh_1, '', content)

        #[LOWDISTINCTNESS(MatchTypeId)]
        re_dh_2 = re.compile(r'\[LOWDISTINCTNESS\(.*\)\]')
        content = re.sub(re_dh_2, '', content)

        #[PARTITION=(PARTITIONCOUNT=2000)]
        re_dh_3 = re.compile(r'\[.+?=\(.+=.+\)\]')
        content = re.sub(re_dh_3, '', content)

        return content

    def remove_split_reserved_char(self, content):
        content = re.sub("';'", '', content)
        content = re.sub('";"', '', content)

        return content

    def remove_ascii_non_target(self, content):
        # such as ASCII 160, 194
        return content.encode('ascii', 'ignore').decode()

    def remove_view_template(self, content):
        re_view = re.compile('.*CREATE VIEW.*?AS BEGIN(.*)END;', re.DOTALL | re.MULTILINE)
        match = re.match(re_view, content)

        if match:
            return match.group(1)

        return content

    def get_module_views(self, content):
        re_module_view = re.compile('.*?VIEW(.*?)RETURN.*?BEGIN(.*?)END VIEW', re.DOTALL | re.MULTILINE)
        occurs = re.findall(re_module_view, content)

        ret = {}
        for occur in occurs:
            view_name, body = occur
            ret[view_name.strip()] = body

        return ret

    def is_input_sstream(self, node):
        if node.name.startswith('SSTREAM_'):
            return True

        return False

    def is_output(self, node):
        if node.attr.get('type', None) == 'output':
            return True

        return False

    def is_int(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False

    def add_sstream_info(self, nodes, declare_map):
        for node in nodes:
            param = ''

            # only target SSTREAM and OUTPUT
            if self.is_input_sstream(node):
                param = node.name[node.name.index('_') + 1:]
            elif self.is_output(node):
                # skip debug files
                if 'debug' in node.name.lower():
                    continue

                param = node.name
            else:
                continue

            if not param in declare_map:
                self.logger.info('param [{}] not in declare_map, probably local reference. Ignore for now.'.format(param))
                continue

            body_str = declare_map[param]
            # remove date or streamset query
            if isinstance(body_str, str) and '?' in body_str:
                self.logger.info('remove query string of [{}]'.format(body_str))
                body_str = body_str[:body_str.index('?')]

            # change node label to html format for different font size
            # ignore if already inserted href
            if not 'FONT' in node.attr['label']:
                href = '{}{}{}'.format(self.sstream_link_prefix,
                                       body_str.replace('"', '').replace('\n', ''),
                                       self.sstream_link_suffix)

                the_label = node.attr['label']

                if self.b_add_sstream_size:
                    self.logger.debug('trying to get stream size of [{}]'.format(href))
                    the_label = '{} ({})'.format(the_label, self.ssu.get_stream_size(href))

                if self.b_add_sstream_link:
                    the_label = '<{} <BR/> <FONT POINT-SIZE="4">{}</FONT>>'.format(the_label, href)
                #    node.attr['href'] = href # not work when rendered to pdf, works in jupyter

                node.attr['label'] = the_label

        # for highlight PROCESS/REDUCE ... USING
        for node in nodes:
            if 'using' in node.attr and not 'FONT' in node.attr['label']:
                label = '<{} <BR/> <FONT POINT-SIZE="8">-- {} --</FONT>>'.format(node.attr['label'], node.attr['using'])
                node.attr['label'] = label

                node.attr['fillcolor'] = 'yellow'
                node.attr['style'] = 'filled'

    def change_node_color(self, nodes):
        for node in nodes:
            self.logger.debug('node = {}'.format(node))

            if '_' not in node.name:
                continue

            input_type = node.name.split('_')[0]

            if input_type not in ['SSTREAM', 'SSTREAM<STREAMSET>', 'EXTRACT', 'MODULE', 'VIEW', 'FUNC']:
                continue

            attr = node.attr
            attr.update({'type': 'input',
                         'style': 'filled'})

            # color scheme: https://www.graphviz.org/doc/info/colors.html#brewer
            if input_type == 'SSTREAM':
                attr['fillcolor'] = 'greenyellow'
            elif input_type == 'SSTREAM<STREAMSET>':
                attr['fillcolor'] = 'wheat'
            elif input_type == 'EXTRACT':
                attr['fillcolor'] = 'honeydew'
            elif input_type == 'MODULE':
                attr['fillcolor'] = 'sandybrown'
            elif input_type == 'VIEW':
                attr['fillcolor'] = 'lightpink'
            elif input_type == 'FUNC':
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

    def process_import(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_input_module(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.input.parse(part))

    def process_process(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.process.parse(part))

    def process_reduce(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.reduce.parse(part))

    def process_combine(self, part, node_map, all_nodes, edges):
        self.process_core(part, node_map, all_nodes, edges, self.combine.parse(part))

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

            if '.' in source:
                main_node_name = source.split('.')[0]

                if main_node_name in node_map:
                    edges.append(Edge(node_map[main_node_name], node_map[source]))

        if len(from_nodes) == 0:
            from_nodes.append(node_map['last_node'])

        if d['assign_var']:
            attr = {}
            node_name = d['assign_var']

            if node_name in node_map:
                attr = node_map[node_name].attr

            # for those like PROCESS ... USING
            if 'using' in d:
                attr['using'] = d['using']

            new_node = Node(node_name, attr=attr)
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

        # ignore MAP for now
        if 'MAP' in value:
            declare_map['@' + key] = 'MAP'

        # back-to-back double quotes are from resolving external params
        # case: "@@ExtParam@@" with @@ExtParam@@ = \"some_string\"
        declare_map['@' + key] = value.replace('""', '"')

        self.logger.info('declare [{}] as [{}]'.format(key, value))

    def process_set(self, part, declare_map):
        key, value = self.set.parse(part)

        if 'IF' in value:
            self.logger.info('for now, we do not handle IF statement.')
            return

        declare_map['@' + key] = value

        self.logger.info('set [{}] as [{}]'.format(key, value))

    def update_module_view_data(self, final_nodes, final_edges, nodes, edges, view_name):
        processed = set()

        for node in nodes:
            # nodes may be duplicate because we recorded the whole appearance
            if node in processed:
                continue

            node.name = '<{}>_{}'.format(view_name, node.name)
            processed.add(node)

        final_nodes.extend(nodes)
        final_edges.extend(edges)

    def parse_file(self, filepath, external_params={}, dest_filepath=None):
        self.logger.info('parse_file [{}]'.format(filepath))
        self.logger.debug('external_params = {}'.format(external_params))

        # keep date key because external params from config is probably yyyy-MM-dd format
        for key in external_params:
            if 'date' in key.lower() or 'hour' in key.lower():
                if 'yyyy' in external_params[key] or 'mmdd' in external_params[key]:
                    continue

            self.external_params[key] = external_params[key]
            self.logger.debug('update external_param key [{}] to value [{}]'.format(key, self.external_params[key]))

        content = FileUtility.get_file_content(filepath)

        final_nodes = []
        final_edges = []

        if filepath.endswith('.module'):
            d = self.get_module_views(content)

            for view_name in d:
                content = d[view_name]
                nodes, edges = self.parse_content(content, external_params)

                self.update_module_view_data(final_nodes, final_edges, nodes, edges, view_name)

        if filepath.endswith('.view'):
            content = self.remove_view_template(content)

            final_nodes, final_edges = self.parse_content(content, external_params)

        if filepath.endswith('.script'):
            final_nodes, final_edges = self.parse_content(content, external_params)

        if dest_filepath:
            self.to_graph(dest_filepath, final_nodes, final_edges)

        # save cosmos querying results
        self.ssu.refresh_cache()

    def parse_content(self, content, external_params={}):
        content = self.remove_comments(content)
        content = self.remove_if(content)
        content = self.remove_if(content) # for nested if
        content = self.resolve_external_params(content, self.external_params)
        content = self.expand_loop(content)
        content = self.remove_data_hint(content)
        content = self.remove_split_reserved_char(content)
        content = self.remove_ascii_non_target(content)

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
            elif 'IMPORT' in part:
                self.logger.info('not support IMPORT for now.')
            elif 'OUTPUT' in part:
                self.process_output(part, node_map, all_nodes, edges)
            elif 'REDUCE' in part:
                self.process_reduce(part, node_map, all_nodes, edges)
            elif 'COMBINE' in part:
                self.process_combine(part, node_map, all_nodes, edges)
            elif 'SELECT' in part:
                self.process_select(part, node_map, all_nodes, edges)
            elif 'SSTREAM' in part:
                self.process_input_sstream(part, node_map, all_nodes, edges)
            elif 'EXTRACT' in part:
                self.process_extract(part, node_map, all_nodes, edges)
            elif 'VIEW' in part:
                self.process_view(part, node_map, all_nodes, edges)
            elif 'PROCESS' in part:
                self.process_process(part, node_map, all_nodes, edges)
            else:
                try:
                    self.process_input_module(part, node_map, all_nodes, edges)
                except Exception as ex:
                    self.logger.warning(ex)
                    pass

        self.logger.info(declare_map)

        self.scope_resolver.resolve_declare(declare_map)

        self.logger.info('change node color for output')
        self.change_node_color(all_nodes)

        if self.b_add_sstream_link or self.b_add_sstream_size:
            self.add_sstream_info(all_nodes, declare_map)

        return all_nodes, edges

    def to_graph(self, dest_filepath, nodes, edges):
        gu = GraphUtility(nodes, edges)

        gexf_output_file = gu.to_gexf_file(dest_filepath)
        self.logger.info('output .gexf file to [{}]'.format(gexf_output_file))

        dot_output_file = gu.to_dot_file(dest_filepath)
        self.logger.info('output .dot file to [{}]'.format(dot_output_file))

        self.logger.info('render graphviz file')
        gu.dot_to_graphviz(dot_output_file)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    result = ScriptParser().expand_loop('''
        #DECLARE Date5 string = string.Format("{0:yyyyMMdd}", @EndDate_Comp.AddDays(-1));
        #DECLARE Date6 string = string.Format("{0:yyyyMMdd}", @EndDate_Comp);
        
        LOOP(a, 2){
            Hour@@a@@ = SELECT @Date0 AS DateKey, string.Format("@@a@@") AS HourKey, 1 AS Tag FROM EmptyFile;
        }
            
        Full0 = SELECT * FROM Hour23
        LOOP(b, 3)
        {
            UNION ALL
            SELECT * FROM Hour@@b@@
        };
        
        LOOP(a, 2){
            Hour@@a@@ = SELECT @Date1 AS DateKey, string.Format("@@a@@") AS HourKey, 1 AS Tag FROM EmptyFile;
        }
            
        LOOP(b, @KK)
        {
            UNION ALL
            SELECT * FROM Hour@@b@@
        };

    ''')


    print(result)


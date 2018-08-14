import xml.etree.ElementTree as ET
import logging
import re
import os
import codecs
from util.file_utility import FileUtility

from graph.node import Node
from graph.edge import Edge
from graph.graph_utility import GraphUtility

class WorkflowObj(object):
    def __init__(self):
        self.masters = {}  # config_filename -> master config dict
        self.workflows = {}  # process_name -> workflow config dict
        self.process_filepath = {}  # process_name -> filepath
        self.wf_groups = {}  # group_name -> list of process_name
        self.process_event_deps = {}  # process_name -> list of depends on events
        self.master_wf_groups = {}  # config_filename -> list of group names
        self.process_master_map = {} # process_name -> master config filename
        self.process_group_map = {}  # process_name -> group name
        self.group_master_map = {}   # group name -> master config name


class WorkflowParser(object):
    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def is_master_config(self, root):
        return root.find('SqlConnectionString') is not None

    def parse_master_config(self, root):
        ret = {'parameters': {},
               'workflows': {},
               'master': True}

        for param in root.find('Parameters'):
            ret['parameters'][param.find('Name').text] = param.find('Value').text

        for workflow in root.find('Workflows'):
            ret['workflows'][workflow.find('Process').text] = workflow.find('Group').text

        return ret

    def parse_workflow_config(self, root):
        ret = {'master': False}

        process_name = root.find('Process').text
        class_name = root.find('ClassName').text

        ret['process_name'] = process_name
        ret['class_name'] = class_name

        for param in root.find('Parameters'):
            name = param.find('Name').text

            if name in ['EventNamesToCheck', 'ScopeJobParams']:
                ret[name] = []
                for item in param.find('Value'):
                    ret[name].append(item.text)
            else:
                ret[name] = param.find('Value').text

        return ret

    def parse_file(self, filepath):
        root = ET.parse(filepath).getroot()

        try:
            b_master = self.is_master_config(root)

            if b_master:
                return self.parse_master_config(root)
            else:
                return self.parse_workflow_config(root)
        except Exception as ex:
            self.logger.debug('{}: {}'.format(filepath, ex))

    def parse_folder(self, folder_root, exclude_keys=[]):
        files = FileUtility.list_files_recursive(folder_root, target_suffix='.config')

        masters = {}            # config_filename -> master config dict
        workflows = {}          # process_name -> workflow config dict
        process_filepath = {}   # process_name -> filepath
        wf_groups = {}          # group_name -> list of process_name
        process_event_deps = {} # process_name -> list of depends on events
        master_wf_groups = {}   # config_filename -> list of group names
        process_master_map = {} # process_name -> master config filename
        process_group_map = {}  # process_name -> group name
        group_master_map = {}   # group name -> master config name

        exclude_keys.append('/objd/')

        for filepath in files:
            for key in exclude_keys:
                if key in filepath:
                    self.logger.info('skip exclude file [{}]'.format(filepath))
                    continue

            self.logger.debug('parse_folder: filepath = {}'.format(filepath))
            try:
                d = self.parse_file(filepath)
            except Exception as e:
                self.logger.warning('skip wrongly parsed file [{}]: {}'.format(filepath, e))
                continue

            if d is None:
                continue

            if d['master']:
                key = os.path.basename(filepath)
                masters[key] = d
                master_wf_groups[key] = set()

                for process_name in d['workflows']:
                    group = d['workflows'][process_name]

                    if not group in wf_groups:
                        wf_groups[group] = []

                    wf_groups[group].append(process_name)

                    if not group in master_wf_groups[key]:
                        master_wf_groups[key].add(group)
                        group_master_map[group] = key

                    process_group_map[process_name] = group
                    process_master_map[process_name] = key
            else:
                process_name = d['process_name']
                class_nam = d['class_name']

                if not 'ScopeJobRunner' in class_nam:
                    self.logger.info('skip non-ScopeJobRunner config.')
                    continue

                script_name = os.path.basename(d['ScriptFile']).replace('.script', '')
                config_name = os.path.basename(filepath).replace('.config', '')

                if config_name != process_name:
                    self.logger.warning('config_name != script_name, use config_name as process_name')
                    process_name = config_name
                    d['process_name'] = config_name

                self.logger.debug('process_name = {}'.format(process_name))
                workflows[process_name] = d
                process_filepath[process_name] = filepath

                if 'EventNamesToCheck' in d:
                    process_event_deps[process_name] = d['EventNamesToCheck']
                else:
                    process_event_deps[process_name] = ('None',)

        obj = WorkflowObj()

        obj.workflows = workflows
        obj.master_wf_groups = master_wf_groups
        obj.process_event_deps = process_event_deps
        obj.wf_groups = wf_groups
        obj.process_filepath = process_filepath
        obj.masters = masters
        obj.group_master_map = group_master_map
        obj.process_group_map = process_group_map
        obj.process_master_map = process_master_map

        return obj

    def print_obj(self, workflow_obj):
        obj = workflow_obj

        for key in obj.masters:
            print(key)

            for group in obj.master_wf_groups[key]:
                print('\t{}'.format(group))

                for process_name in obj.wf_groups[group]:
                    if process_name in obj.workflows:
                        print('\t\t{} -> ({})'.format(process_name, os.path.basename(obj.workflows[process_name]['ScriptFile'])))

                    try:
                        for event in obj.process_event_deps[process_name]:
                            print('\t\t\t{}'.format(event))
                    except Exception as ex:
                        self.logger.warning(ex)

    def resolve_param(self, master_params, param_str):
        for match in re.findall(r'\$\(.*?\)', param_str):
            param = match[2:-1]

            if param in master_params:
                param_str = param_str.replace(match, master_params[param])

        return param_str.replace('\\"', '"')

    def print_params(self, workflow_obj, process_name, resolve=False):
        obj = workflow_obj

        master_key = obj.process_master_map[process_name]
        master_params = obj.masters[master_key]['parameters']
        job_params = obj.workflows[process_name]['ScopeJobParams']

        for item in job_params:
            if not '-params' in item:
                continue

            if resolve:
                print(self.resolve_param(master_params, item.split()[1].strip()))
            else:
                print(item)

    def normalize_event_name(self, event_name: str):
        match = re.match(r'(d\:)?([^/]*)(/.*)?', event_name)

        if match:
            normalized_event_name = match.group(2)
            self.logger.debug('normalize_event_name from [{}] to [{}]'.format(event_name, normalized_event_name))

            return normalized_event_name

        return event_name

    def change_node_color(self, nodes):
        for node in nodes:
            attr = node.attr

            type_ = attr['type']

            # color scheme: https://www.graphviz.org/doc/info/colors.html#brewer
            if type_ == 'EVENT':
                attr['style'] = 'filled'
                attr['fillcolor'] = 'lightblue'

    def get_target_objects(self, adj_map, rev_map, nodes_map, target_node_names=[]):
        target_map = {}

        # init
        for node_name in target_node_names:
            target_map[node_name] = set(['up', 'down'])

            # highlight target nodes
            nodes_map[node_name].attr['style'] = 'filled'
            nodes_map[node_name].attr['fillcolor'] = 'yellow'

        # reconstruct edges from nodes
        nodes = set()
        edges = set()

        while len(target_map) > 0:
            target_name, mode_list = target_map.popitem()

            the_node = nodes_map[target_name]
            nodes.add(the_node)

            if 'down' in mode_list:
                if not target_name in adj_map:
                    continue

                for to_node in adj_map[target_name]:
                    nodes.add(to_node)

                    if to_node.name not in target_map:
                        target_map[to_node.name] = set()

                    target_map[to_node.name].add('down')
                    edges.add(Edge(the_node, to_node))

            if 'up' in mode_list:
                if not target_name in rev_map:
                    continue

                for from_node in rev_map[target_name]:
                    nodes.add(from_node)

                    if from_node.name not in target_map:
                        target_map[from_node.name] = set()

                    target_map[from_node.name].add('up')
                    edges.add(Edge(from_node, the_node))

        return nodes, edges

    def update_adj_map(self, src_node, dest_node, adj_map, rev_map):
        src_name = src_node.name
        dest_name = dest_node.name

        if not src_name in adj_map:
            adj_map[src_name] = set()

        if not dest_name in rev_map:
            rev_map[dest_name] = set()

        adj_map[src_name].add(dest_node)
        rev_map[dest_name].add(src_node)

    def to_workflow_dep_graph(self, workflow_obj, dest_filepath=None, target_node_names=[]):
        obj = workflow_obj

        event_deps = obj.process_event_deps
        workflows = obj.workflows

        nodes_map = {}
        edges = []

        adj_map = {}
        rev_map = {}

        for process_name in workflows:
            script_name = os.path.basename(workflows[process_name]['ScriptFile'])

            script_attr = {
                'id': script_name,
                'label': script_name,
                'type': 'SCRIPT'
            }

            if not script_name in nodes_map:
                script_node = Node(script_name, attr=script_attr)
                nodes_map[script_name] = script_node

            script_node = nodes_map.get(script_name)

            # the output event of this process
            output_event_name = workflows[process_name]['EventName']

            if not output_event_name in nodes_map:
                script_out_event_node = Node(output_event_name, attr={'id': output_event_name,
                                                        'label': output_event_name,
                                                        'type': 'EVENT'})
                nodes_map[output_event_name] = script_out_event_node

            script_out_event_node = nodes_map.get(output_event_name)
            # add edge
            edges.append(Edge(script_node, script_out_event_node))
            # for target filtering
            self.update_adj_map(script_node, script_out_event_node, adj_map, rev_map)

            # input events
            dep_events = event_deps[process_name]

            for event_name in dep_events:
                # to make the graph less complex
                normalized_event_name = self.normalize_event_name(event_name)

                if not normalized_event_name in nodes_map:
                    event_attr = {
                        'id': normalized_event_name,
                        'label': normalized_event_name,
                        'type': 'EVENT'
                    }

                    script_in_event_node = Node(normalized_event_name, attr=event_attr)
                    nodes_map[normalized_event_name] = script_in_event_node

                script_in_event_node = nodes_map.get(normalized_event_name)

                # add edge
                edges.append(Edge(script_in_event_node, script_node))
                # for target filtering
                self.update_adj_map(script_in_event_node, script_node, adj_map, rev_map)

        self.logger.debug('node_map.keys = {}'.format(nodes_map.keys()))

        if target_node_names:
            nodes, edges = self.get_target_objects(adj_map, rev_map, nodes_map, target_node_names=target_node_names)
        else:
            nodes = nodes_map.values()

        if dest_filepath:
            self.logger.info('change node color for output')
            self.change_node_color(nodes)

            gu = GraphUtility(nodes, edges)

            gexf_output_file = gu.to_gexf_file(dest_filepath)
            self.logger.info('output .gexf file to [{}]'.format(gexf_output_file))

            dot_output_file = gu.to_dot_file(dest_filepath)
            self.logger.info('output .dot file to [{}]'.format(dot_output_file))

            self.logger.info('render graphviz file')
            gu.dot_to_graphviz(dot_output_file)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    import json

    proj_name = 'Opportunities'
    exclude_keys = []
    target_node_names = ['5.FinalCapping.script', 'MPIPrepare.script', '6.MPIProcessing0.script']
#    target_node_names = []

#    proj_name = 'BTE'
#    exclude_keys = ['NKW/', 'NKW2/']

    wfp = WorkflowParser()
    obj = wfp.parse_folder(r'D:\workspace\AdInsights\private\Backend\{}'.format(proj_name), exclude_keys=exclude_keys)

#    wfp.print_params(obj, 'SOV3_StripeOutput', resolve=True)
    wfp.to_workflow_dep_graph(obj, 'd:/tmp/event_dep_{}_[{}]'.format(proj_name, '-'.join(target_node_names)), target_node_names=target_node_names)

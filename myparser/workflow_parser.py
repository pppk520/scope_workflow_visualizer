import xml.etree.ElementTree as ET
import logging
import re
import os
import codecs
import editdistance
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
        self.script_process_map = {} # script name -> process_name
        self.event_interval_map = {}  # event_name -> interval

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

    def normalized_delta_interval(self, interval_str):
        ''' Only support D and H

        1.00:00:00  -> 1D
        2.00:00:00  -> 2D
        P1D         -> 1D
        P2D         -> 2D
        03:00:00    -> 3H

        :param interval_str: such as 2.00:00:00
        :return: string '2D'
        '''

        if interval_str == 'P1D': return '1D'
        if interval_str == 'P2D': return '2D'

        if '.' in interval_str:
            return interval_str[:interval_str.index('.')] + 'D'

        return interval_str[:interval_str.index(':')].lstrip('0') + 'H'


    def get_closest_process_name(self, process_key, workflow_obj):
        self.logger.debug('get_closest_process_name, process_key [{}]'.format(process_key))

        process_names = workflow_obj.workflows.keys()
        script_process_map = workflow_obj.script_process_map

        if process_key in process_names:
            return process_key

        if process_key in script_process_map:
            return script_process_map[process_key]

        process_key_core = os.path.splitext(process_key)[0]
        # such as key [NKWOptMPIProcessing] and script [NKWOptMPIProcessing3.script]
        for script_name in script_process_map:
            if process_key_core in script_name:
                return script_process_map[script_name]

        # use edit distance to identify the closest one
        min_key = process_key
        min_dist = len(process_key) * 2  # just a big enough value

        for key in process_names:
            dist = editdistance.eval(key, process_key)
            if dist < min_dist:
                min_dist = dist
                min_key = key

        return min_key

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
        script_process_map = {} # script name -> process_name
        event_interval_map = {} # event_name -> interval

        exclude_keys.append('/objd/') # by default, ignore this

        for filepath in files:
            b_exclude = False
            for key in exclude_keys:
                if key in filepath:
                    self.logger.info('skip exclude file [{}]'.format(filepath))
                    b_exclude = True
                    break

            if b_exclude:
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
                self.logger.info('found master config [{}]'.format(filepath))
                filename = os.path.basename(filepath)
                folder_name = os.path.dirname(filepath)
                key = folder_name + filename
                self.logger.info('master config key = [{}]'.format(key))

                if key in masters:
                    self.logger.info('only use the first occurrence of a master config')
                    continue

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

                script_name = os.path.basename(d['ScriptFile'])
                script_name_key = script_name.replace('.script', '')
                config_name = os.path.basename(filepath).replace('.config', '')

                if config_name != process_name:
                    self.logger.warning('config_name != script_name_key, use config_name as process_name')
                    process_name = config_name
                    d['process_name'] = config_name

                # keep the first occurrence only
                if script_name not in script_process_map:
                    script_process_map[script_name] = d['process_name']

                self.logger.debug('process_name = {}'.format(process_name))
                workflows[process_name] = d
                process_filepath[process_name] = filepath

                if 'EventNamesToCheck' in d:
                    process_event_deps[process_name] = d['EventNamesToCheck']
                else:
                    process_event_deps[process_name] = ('None',)

                # keep the interval of this event
                if 'EventName' in d:
                    event_interval_map[d['EventName']] = self.normalized_delta_interval(d['DeltaInterval'])

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
        obj.script_process_map = script_process_map
        obj.event_interval_map = event_interval_map

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
        self.logger.debug('resolve_param of [{}]'.format(param_str))

        for match in re.findall(r'\$\(.*?\)', param_str):
            param = match[2:-1]

            if param in master_params:
                if master_params[param] is None:
                    continue

                param_str = param_str.replace(match, master_params[param])

        return param_str.replace('\\"', '"')

    def get_params(self, workflow_obj, process_name):
        obj = workflow_obj

        if not process_name in obj.process_master_map:
            process_name = self.get_closest_process_name(process_name, obj)

        master_key = obj.process_master_map[process_name]
        master_params = obj.masters[master_key]['parameters']
        job_params = obj.workflows[process_name]['ScopeJobParams']

        param_map = {}

        for item in job_params:
            if not '-params' in item:
                continue

            _, target = item.split()
            # one params can map multiple params
            # e.g. -params Date=\"{yyyy-MM-dd}\",hour={HH}

            if ',' in target:
                targets = target.split(',')
            else:
                targets = [target,]

            for target in targets:
                key, value = target.split('=')
                param_map[key] = self.resolve_param(master_params, value.strip())

        return param_map

    def print_params(self, workflow_obj, process_name, resolve=False):
        param_map = self.get_params(workflow_obj, process_name)

        for key in param_map:
            print('{} = {}'.format(key, param_map[key]))


    def normalize_event_name(self, event_name: str):
        match = re.match(r'([dD]\:)?([^/]*)(/.*)?', event_name)

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

                interval = attr.get('interval', 'NA')

                if interval.endswith('D'):
                    attr['fillcolor'] = 'lightblue'
                elif interval.endswith('H'):
                    attr['fillcolor'] = 'palegoldenrod'
                else:
                    # external
                    attr['fillcolor'] = 'gray'

    def filter_objects(self, adj_map, rev_map, nodes_map, type_='SCRIPT'):
        ''' To remove event nodes, reserve script mapping only

        current graph is script -> event -> script -> event -> ...

        :param adj_map: adjacency map (a -> (node_b, node_c))
        :param rev_map: reverse map (b -> (node_a, node_d))
        :param nodes_map: contains all nodes by now
        :return: the new nodes and edges
        '''

        # reconstruct edges from nodes
        nodes = set()
        edges = set()

        for from_name in adj_map:
            from_node = nodes_map[from_name]

            # always keep root event node
            if from_node.attr['type'] == 'EVENT' and from_name not in rev_map:
                nodes.add(from_node)

                for to_node in adj_map[from_name]:
                    edges.add(Edge(from_node, to_node))

            if from_node.attr['type'] != type_:
                continue

            nodes.add(from_node)

            for to_node in adj_map[from_name]:
                to_name = to_node.name

                # map child's children to parent
                if to_name in adj_map:
                    for grand_to_node in adj_map[to_name]:
                        edges.add(Edge(from_node, grand_to_node))

        return nodes, edges

    def get_target_objects(self, adj_map, rev_map, nodes_map, target_node_names=[]):
        ''' To reserve only nodes related to target nodes

        The relationship is defined as up(direct parent node), down(all children node)

        :param adj_map: adjacency map (a -> (node_b, node_c))
        :param rev_map: reverse map (b -> (node_a, node_d))
        :param nodes_map: contains all nodes by now
        :param target_node_names: if specified, only trace 'related' nodes
        :return: the new nodes and edges
        '''
        target_map = {}

        # init
        for node_name in target_node_names:
            if node_name not in nodes_map:
                self.logger.info('specified node [{}] not in node_map. skip.'.format(node_name))
                continue

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

    def to_workflow_dep_graph(self,
                              workflow_obj,
                              dest_filepath=None,
                              target_node_names=[],
                              filter_type=None):
        obj = workflow_obj

        event_deps = obj.process_event_deps
        workflows = obj.workflows
        process_master_map = obj.process_master_map
        event_interval_map = obj.event_interval_map

        nodes_map = {}
        edges = []

        adj_map = {}
        rev_map = {}

        for process_name in workflows:
            # only show those enabled in master config
            if process_name not in process_master_map:
                self.logger.debug('skip process [{}] because it is not in master config.'.format(process_name))
                continue

            script_fullpath = workflows[process_name]['ScriptFile']
            script_name = os.path.basename(script_fullpath)

            script_attr = {
                'id': script_name,
                'label': script_name,
                'type': 'SCRIPT',
                'tooltip': script_fullpath
            }

            if not script_name in nodes_map:
                script_node = Node(script_name, attr=script_attr)
                nodes_map[script_name] = script_node

            script_node = nodes_map.get(script_name)

            # the output event of this process
            output_event_name = workflows[process_name]['EventName']

            if output_event_name not in nodes_map:
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

                if normalized_event_name not in nodes_map:
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

        # update event interval
        for node_name in nodes_map:
            if nodes_map[node_name].attr['type'] == 'EVENT':
                # external event
                if node_name not in event_interval_map:
                    continue

                interval = event_interval_map[node_name]

                nodes_map[node_name].attr['label'] += ' ({})'.format(interval)
                nodes_map[node_name].attr['interval'] = interval

        self.logger.debug('node_map.keys = {}'.format(nodes_map.keys()))

        if target_node_names:
            nodes, edges = self.get_target_objects(adj_map,
                                                   rev_map,
                                                   nodes_map,
                                                   target_node_names=target_node_names)
        else:
            nodes = nodes_map.values()

        # for now not support target_node_names go together with filter
        if filter_type:
            nodes, edges = self.filter_objects(adj_map, rev_map, nodes_map, type_=filter_type)

        if dest_filepath:
            self.logger.info('change node color for output')
            self.change_node_color(nodes)

            gu = GraphUtility(nodes, edges)

            gexf_output_file = gu.to_gexf_file(dest_filepath)
            self.logger.info('output .gexf file to [{}]'.format(gexf_output_file))

            dot_output_file = gu.to_dot_file(dest_filepath)
            self.logger.info('output .dot file to [{}]'.format(dot_output_file))

            self.logger.info('render graphviz file pdf')
            gu.dot_to_graphviz(dot_output_file, format='pdf')
            self.logger.info('render graphviz file svg')
            gu.dot_to_graphviz(dot_output_file, format='svg')




if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    import json

    proj_name = 'Opportunities'
    exclude_keys = []
#    target_node_names = ['5.FinalCapping.script', 'MPIPrepare.script', '6.MPIProcessing0.script']
    target_node_names = ['MPIPrepare.script']
#    target_node_names = []

#    proj_name = 'BTE'
#    exclude_keys = ['NKW/', 'NKW2/']
#    target_node_names = ['BidForPosition.script']

    wfp = WorkflowParser()
    obj = wfp.parse_folder(r'D:\workspace\AdInsights\private\Backend\{}'.format(proj_name), exclude_keys=exclude_keys)

    wfp.print_params(obj, 'MPIPrepare.script')
#    wfp.to_workflow_dep_graph(obj, 'd:/tmp/event_dep_{}_[{}]'.format(proj_name, '-'.join(target_node_names)), target_node_names=target_node_names)

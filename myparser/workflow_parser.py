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
        try:
            return event_name[:event_name.index('/')]
        except:
            return event_name

    def to_workflow_dep_graph(self, workflow_obj, dest_filepath=None):
        obj = workflow_obj

        event_deps = obj.process_event_deps
        workflows = obj.workflows

        nodes = {}
        edges = []

        for process_name in workflows:
            script_name = os.path.basename(workflows[process_name]['ScriptFile'])

            script_attr = {
                'id': script_name,
                'label': script_name,
                'type': 'SCRIPT'
            }

            if not script_name in nodes:
                script_node = Node(script_name, attr=script_attr)
                nodes[script_name] = script_node

            script_node = nodes.get(script_name)

            # the output event of this process
            output_event_name = workflows[process_name]['EventName']

            if not output_event_name in nodes:
                script_out_event_node = Node(output_event_name, attr={'id': output_event_name,
                                                        'label': output_event_name,
                                                        'type': 'EVENT'})
                nodes[output_event_name] = script_out_event_node

            script_out_event_node = nodes.get(output_event_name)
            edges.append(Edge(script_node, script_out_event_node))

            # input events
            dep_events = event_deps[process_name]

            for event_name in dep_events:
                # to make the graph less complex
                normalized_event_name = self.normalize_event_name(event_name)

                if not normalized_event_name in nodes:
                    event_attr = {
                        'id': normalized_event_name,
                        'label': normalized_event_name,
                        'type': 'EVENT'
                    }

                    script_in_event_node = Node(normalized_event_name, attr=event_attr)
                    nodes[normalized_event_name] = script_in_event_node

                script_in_event_node = nodes.get(normalized_event_name)

                edges.append(Edge(script_in_event_node, script_node))

        if dest_filepath:
            self.logger.info('output GEXF to [{}]'.format(dest_filepath))
            GraphUtility().to_gexf_file(nodes.values(), edges, dest_filepath)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    import json

    proj_name = 'SOV'
    exclude_keys = []

#    proj_name = 'BTE'
#    exclude_keys = ['NKW/', 'NKW2/']

    wfp = WorkflowParser()
    obj = wfp.parse_folder(r'D:\workspace\AdInsights\private\Backend\{}'.format(proj_name), exclude_keys=exclude_keys)

    wfp.print_params(obj, 'SOV3_StripeOutput', resolve=True)
#    wfp.to_workflow_dep_graph(obj, 'd:/tmp/event_dep_{}.gexf'.format(proj_name))

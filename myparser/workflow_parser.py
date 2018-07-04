import xml.etree.ElementTree as ET
import logging
import re
import os
import codecs
from util.file_utility import FileUtility

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

        ret['process_name'] = process_name

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

    def parse_folder(self, folder_root):
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

        for filepath in files:
            self.logger.debug('parse_folder: filepath = {}'.format(filepath))
            d = self.parse_file(filepath)

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
                script_name = os.path.basename(d['ScriptFile']).replace('.script', '')

                if script_name != process_name:
                    self.logger.warning('process_name != script_name, use script_name as process_name')
                    process_name = script_name
                    d['process_name'] = script_name

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


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    import json

    wfp = WorkflowParser()
#    obj = wfp.parse_file(r'D:\workspace\AdInsights\private\Backend\SOV\Workflows\MasterConfig\Workflows_master.config')
#    obj = wfp.parse_file(r'D:\workspace\AdInsights\private\Backend\SOV\Workflows\WF_Groups\ADC.SOV.Scope\Config\SOV1_RawData.config')

    obj = wfp.parse_folder(r'D:\workspace\AdInsights\private\Backend\SOV')
#    obj = wfp.parse_folder(r'D:\workspace\AdInsights\private\Common')

#    wfp.print_params(obj, 'SOV3_StripeOutput', resolve=False)
#    wfp.print_params(obj, 'SOV_BSC_LostToCampaignBudget_Raw', resolve=True)
    wfp.print_params(obj, 'AucIns_Final', resolve=True)

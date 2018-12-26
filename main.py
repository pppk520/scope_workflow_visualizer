import click
import os
import logging
import traceback
import json
import sys
import multiprocessing as mp
from myparser.script_parser import ScriptParser
from myparser.workflow_parser import WorkflowParser
from util.file_utility import FileUtility
from datetime import datetime
from util.datetime_utility import DatetimeUtility

__log_level = logging.DEBUG


@click.group()
def cli():
    pass


def parse_script_single(target_filename,
                        workflow_parser,
                        workflow_obj,
                        script_fullpath_map,
                        add_sstream_link,
                        add_sstream_size,
                        output_folder,
                        external_params,
                        master_key=None,
                        target_date_str=None):
    # each process needs to set log level
    logging.basicConfig(level=__log_level)

    wfp = workflow_parser
    obj = workflow_obj

    target_filename = os.path.basename(target_filename)  # make sure it's basename

    try:
        process_name = wfp.get_closest_process_name(target_filename, obj)
        print('target_filename = [{}], closest process_name = [{}]'.format(target_filename, process_name))
        param_map = wfp.get_params(obj, process_name, master_key=master_key)

        sp = ScriptParser(b_add_sstream_link=add_sstream_link,
                          b_add_sstream_size=add_sstream_size)

        dest_filepath = os.path.join(output_folder, target_filename)
        script_fullpath = script_fullpath_map[target_filename]

        if target_date_str:
            datetime_obj = datetime.strptime(target_date_str, '%Y-%m-%d')

            for key in param_map:
                # replace {yyyy-MM-dd}
                param_map[key] = DatetimeUtility.replace_ymd(param_map[key], datetime_obj)

        param_map.update(external_params)

        # dest_filepath will be appended suffix like .dot.pdf
        print('parse_file [{}]'.format(script_fullpath))
        sp.parse_file(script_fullpath, external_params=param_map, dest_filepath=dest_filepath)
    except Exception as ex:
        print('[WARNING] Failed parse file [{}]: {}'.format(target_filename, ex))
        print(traceback.format_exc())



def parse_script(proj_folder,
                 workflow_folder,
                 output_folder,
                 target_script_folder=None,
                 target_filenames=[],
                 add_sstream_link=False,
                 add_sstream_size=False,
                 exclude_keys=[],
                 external_params={},
                 master_key=None,
                 target_date_str=None):

    print('proj_folder [{}]'.format(proj_folder))
    print('workflow_folder [{}]'.format(workflow_folder))

    wfp = WorkflowParser()
    obj = wfp.parse_folder(workflow_folder)

    script_fullpath_map = {}
    for f in FileUtility.list_files_recursive(proj_folder, target_suffix='.script'):
        script_fullpath_map[os.path.basename(f)] = f

    if len(target_filenames) == 0:
        print('no specified target_filenames, check target_script_folder [{}]'.format(target_script_folder))

        if target_script_folder is not None:
            for f in FileUtility.list_files_recursive(target_script_folder, target_suffix='.script'):
                target_filenames.append(os.path.basename(f))
        else:
            print('no specified target_filenames, add all scripts appear in workflows...')
            for script_name in obj.script_process_map:
                print('add script [{}]'.format(script_name))
                target_filenames.append(script_name)

    print('target files:')
    for f in target_filenames:
        print(f)

    if len(target_filenames) == 0:
        print('no target files, abort.')
        return

    if not os.path.isdir(output_folder):
        print('create folder [{}]'.format(output_folder))
        os.makedirs(output_folder)

    arguments_list = []
    for target_filename in target_filenames:
        arguments = (target_filename,
                     wfp,
                     obj,
                     script_fullpath_map,
                     add_sstream_link,
                     add_sstream_size,
                     output_folder,
                     external_params,
                     master_key,
                     target_date_str
                     )

        arguments_list.append(arguments)

    process_no = min(len(target_filenames), 10)

    if process_no == 1:
        parse_script_single(*arguments_list[0])
        return

    pool = mp.Pool(processes=process_no)
    pool.starmap(parse_script_single, arguments_list)


@cli.command()
@click.argument('proj_folder')
@click.argument('workflow_folder')
@click.argument('output_folder')
@click.option('--target_filenames', multiple=True, default=[])
@click.option('--add_sstream_link', type=bool, default=True, help='resolve and add sstream link')
@click.option('--exclude_keys', multiple=True, default=[])
def script_to_graph(proj_folder,
                 workflow_folder,
                 output_folder,
                 target_filenames,
                 add_sstream_link,
                 exclude_keys):

    return parse_script(proj_folder, output_folder, target_filenames, add_sstream_link, exclude_keys)


@click.argument('workflow_folder', type=click.Path(exists=True))
@click.argument('target_filename')
@click.option('--exclude_keys', multiple=True, default=[])
def print_wf_params(workflow_folder, target_filename, exclude_keys=[], master_key=None):
    wfp = WorkflowParser()
    obj = wfp.parse_folder(workflow_folder)

    process_name = wfp.get_closest_process_name(target_filename, obj)
    print(json.dumps(wfp.get_params(obj, process_name, master_key=master_key), indent=4))


@click.argument('proj_folder', type=click.Path(exists=True))
@click.argument('output_folder')
@click.option('--target_folder_name')
@click.option('--target_node_names', multiple=True, default=[])
@click.option('--exclude_keys', multiple=True, default=[])
@click.option('--filter_type', default=None)
def to_workflow_dep_graph(proj_folder,
                          output_folder,
                          target_folder_name=None,
                          target_node_names=[],
                          exclude_keys=[],
                          filter_type=None):
    wfp = WorkflowParser()
    obj = wfp.parse_folder(proj_folder)

    proj_name = os.path.basename(proj_folder)
    dest_filepath = '{}/event_dep_[{}]_target_folders[{}]_nodes[{}]_filter_{}'\
                            .format(output_folder,
                                    proj_name,
                                    target_folder_name,
                                    '-'.join(target_node_names),
                                    filter_type)

    # only support either target_folder_name or target_node_names
    if target_folder_name and len(target_node_names) == 0:
        for f in FileUtility.list_files_recursive(proj_folder, target_suffix='.script'):
            if target_folder_name not in f:
                continue

            target_node_names.append(os.path.basename(f))

    FileUtility.mkdir_p(output_folder)
    wfp.to_workflow_dep_graph(obj,
                              dest_filepath=dest_filepath,
                              target_node_names=target_node_names,
                              filter_type=filter_type)


def all_in_one(dwc_wf_folder, out_folder, target_wf_folders=[], target_filenames=[], pdf_only=True):
    target_date_str = DatetimeUtility.get_datetime(-6, fmt_str='%Y-%m-%d')

    for wf_folder in os.listdir(dwc_wf_folder):
        if target_wf_folders and wf_folder not in target_wf_folders:
            print('wf_folder [{}] not in target list [{}]'.format(wf_folder, target_wf_folders))
            continue

        wf_folder_path = os.path.join(dwc_wf_folder, wf_folder)
        print('wf_folder_path [{}]'.format(wf_folder_path))

        out_sub_folder = os.path.join(out_folder, wf_folder)

#        if os.path.exists(out_sub_folder):
#            print('skip processed folder [{}]'.format(out_sub_folder))
#            continue

        to_workflow_dep_graph(wf_folder_path, out_sub_folder)

        out_script_folder = os.path.join(out_sub_folder, 'script_graph')
        parse_script(wf_folder_path,
                     wf_folder_path,
                     out_script_folder,
                     target_filenames=target_filenames[:],
                     add_sstream_link=False,
                     add_sstream_size=False,
                     target_date_str=target_date_str)

        FileUtility.delete_files_except_ext(out_sub_folder, '.pdf')
        FileUtility.delete_files_except_ext(out_script_folder, '.pdf')


if __name__ == '__main__':
#    cli()
    logging.basicConfig(level=logging.DEBUG)

    all_in_one(r'D:\tt_all\retail\amd64\Backend\DWC\DwcService\WorkflowGroups',
               r'D:/tmp/tt_all_in_one')

#    all_in_one(r'D:\tt_all\retail\amd64\Backend\DWC\DwcService\WorkflowGroups',
#               r'D:/tmp/tt_all_in_one',
#               target_wf_folders=['ADC_Opportunities_Scope'],
#               target_filenames=['5.FinalCapping.script']
#               target_wf_folders=['UCM_Scope', 'AIM_Scope', 'ADC_TopMover_Scope'])


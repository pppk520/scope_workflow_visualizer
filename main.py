import click
import os
import logging
import json
import sys
import multiprocessing as mp
from myparser.script_parser import ScriptParser
from myparser.workflow_parser import WorkflowParser
from util.file_utility import FileUtility

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
                        external_params):
    # each process needs to set log level
    logging.basicConfig(level=__log_level)

    wfp = workflow_parser
    obj = workflow_obj

    target_filename = os.path.basename(target_filename)  # make sure it's basename

    try:
        process_name = wfp.get_closest_process_name(target_filename, obj)
        print('target_filename = [{}], closest process_name = [{}]'.format(target_filename, process_name))
        param_map = wfp.get_params(obj, process_name)

        sp = ScriptParser(b_add_sstream_link=add_sstream_link,
                          b_add_sstream_size=add_sstream_size)

        dest_filepath = os.path.join(output_folder, target_filename)
        script_fullpath = script_fullpath_map[target_filename]

        param_map.update(external_params)

        # dest_filepath will be appended suffix like .dot.pdf
        sp.parse_file(script_fullpath, external_params=param_map, dest_filepath=dest_filepath)
    except Exception as ex:
        print('[WARNING] Failed parse file [{}]: {}'.format(target_filename, ex))


def parse_script(proj_folder,
                 workflow_folder,
                 output_folder,
                 target_filenames=[],
                 add_sstream_link=False,
                 add_sstream_size=False,
                 exclude_keys=[],
                 external_params={}):

    wfp = WorkflowParser()
    obj = wfp.parse_folder(workflow_folder)

    script_fullpath_map = {}
    for f in FileUtility.list_files_recursive(proj_folder, target_suffix='.script'):
        script_fullpath_map[os.path.basename(f)] = f

    if len(target_filenames) == 0:
        print('no specified target_filenames, add all scripts appear in workflows...')

        for script_name in obj.script_process_map:
            print('add script [{}]'.format(script_name))
            target_filenames.append(script_name)

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
                     external_params
                     )

        arguments_list.append(arguments)

    pool = mp.Pool(processes=10)
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
def print_wf_params(workflow_folder, target_filename, exclude_keys=[]):
    wfp = WorkflowParser()
    obj = wfp.parse_folder(workflow_folder)

    process_name = wfp.get_closest_process_name(target_filename, obj)
    print(json.dumps(wfp.get_params(obj, process_name), indent=4))


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

    wfp.to_workflow_dep_graph(obj,
                              dest_filepath=dest_filepath,
                              target_node_names=target_node_names,
                              filter_type=filter_type)

if __name__ == '__main__':
#    cli()

#    print_wf_params(r'D:\workspace\AdInsights\private\Backend\SOV', 'SOV3_StripeOutput.script')

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/SOV',
                 r'D:/workspace/AdInsights/private/Backend/SOV',
                 r'D:/tmp/tt',
                 target_filenames=[
#                     'SOV3_StripeOutput.script',
                     'SOV_BSC_Final.script',
#                     'EligibleRGuids.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend\Opportunities',
                 r'D:/tt_all/retail/amd64/Backend/DWC/DwcService/WorkflowGroups/ADC_Opportunities_Scope',
                 r'D:/tmp/tt',
                 target_filenames=[
                     '1.MergeSources.script',
#                     '2.QualtiyControlStep1.script',
#                     '2.QualtiyControlStep2.script',
#                     '3.AssignOptType.script',
#                     '4.TrafficEstimation.script',
#                     '5.FinalCapping.script',
#                     '6.MPIProcessing.script',
#                     '7.PKVGeneration_BMMO.script',
#                     '7.PKVGeneration_BMO.script',
#                     '7.PKVGeneration_BMOEX.script',
#                     '7.PKVGeneration_KWO.script',
#                     'MPIPrepare.script',
#                     'CampaignTargetingInfo.script',
#                     'KeywordOpt_CampaignTargetInfo.script',
#                     'NKWOptMPIProcessing.script',
#                     'BudgetOptMPIProcessing.script',
#                     'BudgetOptPKVGeneration.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''


    '''
    to_workflow_dep_graph(
                 r'D:/workspace/AdInsights/private/Backend\Opportunities',
                 r'D:/tmp/tt',
                 target_folder_name='BudgetSuggestions'
    )
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/FeatureAdoption',
                 r'D:/tmp/tt',
                 target_filenames=[
                     'SiteLinkOpportunity.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    to_workflow_dep_graph(
                 r'D:/workspace/AdInsights/private/Backend/UCM',
                 r'D:/tmp/tt',
                 target_node_names=[])
    '''

    # it uses external parameters, use cloudbuild result because it resolves external params
    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/BTE',
                 r'D:/tt_all/retail/amd64/Backend/DWC/DwcService/WorkflowGroups/ADC_BTE_Scope',
                 r'D:/tmp/tt',
                 target_filenames=[
                     'BidOptMPIPreProcessing.script',
                     'BidOptMPIProcessing.script',
                     'BidOptAggregation.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    to_workflow_dep_graph(
                 r'D:/workspace/AdInsights/private/Backend/BTE',
                 r'D:/tmp/tt',
                 target_node_names=[])
    '''


    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/BTE',
                 r'D:/tt_all/retail/amd64/Backend/DWC/DwcService/WorkflowGroups/ADC_BTE_Scope',
                 r'D:/tmp/tt',
                 target_filenames=[
                     'NKW3_TrafficEstimation.script',
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    to_workflow_dep_graph(
                 r'D:/workspace/AdInsights/private/Backend/SOV',
                 r'D:/tmp/tt',
#                 target_node_names=['SOV3_StripeOutput.script'],
#                 filter_type='SCRIPT')
                 filter_type=None)
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend\Opportunities',
                 r'D:/tt_all/retail/amd64/Backend/DWC/DwcService/WorkflowGroups/ADC_Opportunities_Scope',
                 r'D:/tmp/tt',
                 target_filenames=[
                     'BudgetOptMPIProcessing.script',
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/AdvertiserIntelligence',
                 r'D:/workspace/AdInsights/private/Backend/AdvertiserIntelligence',
                 r'D:/tmp/tt',
                 target_filenames=[
#                     'CommonFeatureExtractor.script',
                     'FeatureExtractor.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/FeatureAdoption',
                 r'D:/workspace/AdInsights/private/Backend/FeatureAdoption',
                 r'D:/tmp/tt',
                 target_filenames=[
                    'Account_AccountLevelAdoption.script',
                    'Account_AggregatedAdoption.script',
                    'Account_FeatureAdoption.script'
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True)
    '''

    '''
    parse_script(r'D:/workspace/AdInsights/private/Backend/AdvertiserIntelligence',
                 r'D:/workspace/AdInsights/private/Backend/AdvertiserIntelligence',
                 r'D:/tmp/tt',
                 target_filenames=[
                    'PosViewCntAnalyze.script',
                 ],
                 add_sstream_link=True,
                 add_sstream_size=True,
                 external_params={
                     'startDateStr': '2018-09-21',
                     'endDateStr': '2018-09-23',

                 })
    '''

    '''
    to_workflow_dep_graph(
                 r'D:\workspace\AdInsights\private\Backend\AdInsightMad\DWCMeasurement\Deployment\DwcService\WorkflowGroups',
                 r'D:/tmp/tt',
                 target_node_names=['AdInsightNormalizedRUI.script'],
                 filter_type=None)
    '''


    parse_script(r'D:\workspace\AdInsights\private\Backend\AdInsightMad\DWCMeasurement\Deployment\DwcService\WorkflowGroups',
                 r'D:\workspace\AdInsights\private\Backend\AdInsightMad\DWCMeasurement\Deployment\DwcService\WorkflowGroups',
                 r'D:/tmp/tt/mad',
                 target_filenames=['AdInsightNormalizedRUI.script'],
                 add_sstream_link=True,
                 add_sstream_size=True)

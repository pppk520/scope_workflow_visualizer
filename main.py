import click
import os
import logging
from myparser.script_parser import ScriptParser
from myparser.workflow_parser import WorkflowParser
from util.file_utility import FileUtility

@click.group()
def cli():
    pass


@cli.command()
@click.argument('proj_folder', type=click.Path(exists=True))
@click.argument('target_filenames', nargs=-1)
@click.argument('output_folder', type=click.Path(exists=True))
@click.option('--add_sstream_link', type=bool, default=True, help='resolve and add sstream link')
@click.option('--exclude_keys', multiple=True, default=[])
def script_to_graph(proj_folder, output_folder, target_filenames, add_sstream_link, exclude_keys):
    wfp = WorkflowParser()
    obj = wfp.parse_folder(proj_folder)

    script_fullpath_map = {}
    for f in FileUtility.list_files_recursive(proj_folder, target_suffix='.script'):
        script_fullpath_map[os.path.basename(f)] = f

    for target_filename in target_filenames:
        target_filename = os.path.basename(target_filename) # make sure it's basename

        process_name = wfp.get_closest_process_name(target_filename, obj)
        param_map = wfp.get_params(obj, process_name)

        sp = ScriptParser(b_add_sstream_link=add_sstream_link)

        dest_filepath = os.path.join(output_folder, target_filename)
        script_fullpath = script_fullpath_map[target_filename]

        # dest_filepath will be appended suffix like .dot.pdf
        sp.parse_file(script_fullpath, external_params=param_map, dest_filepath=dest_filepath)



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    cli()
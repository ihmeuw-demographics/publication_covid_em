
"""
Description: Submit the covid excess mortality data process jobs using Jobmon
Steps:
    * Build jobmon workflow object with correct tasks
    * Submit workflow
    * Slack upon failure

"""

import argparse
import sys
import jobmon
import getpass
import pandas as pd
import datetime
import rpy2.robjects as robjects
import yaml

# load in functions from jobmon package


# ------------------------------------------------------------------------------


def generate_task(jobname, code, shell, args, fthread, fmem, h_rt, args_shell = "",
                  queue = "all", archive = False, max_attempts = 3,
                  upstream_tasks = []):
    """Generate one jobmon BashTask

    Parameters
    ----------
    jobname : str
        Name of job
    code : str
        Filepath to code file
    shell : str
        Filepath to a shell script to submit the job with
    args_shell : str
        Arguments to pass to shell script.
        Example: "-i image"
    args : str
        Arguments to pass to code.
        Example: "--arg1 arg1 --arg2 arg2"
    fthread : num
        Number of threads to request
    fmem : num
        Number of GB of memory to request
    h_rt : datetime.timedelta
        Wallclock for runtime requested
    queue : str
        Which hostgroup to submit jobs to. Options: "all" or "long"
    archive : bool
        If "True", request a node that has J access
    max_attemps : num
        Number of jobmon resubmit attempts for this task
    upstream_tasks : list
        Tasks that need to complete before this one can begin

    Returns
    -------
    A jobmon BashTask
    """

    # format command
    command = "{shell} {args_shell} -s {code} {args}".format(shell = shell, args_shell = args_shell, code = code, args = args)

    # convert wallclock to seconds
    seconds = h_rt.total_seconds()
    seconds = int(seconds)

    # return BashTask jobmon object
    return BashTask(name = jobname,
                    command = command,
                    num_cores = fthread,
                    m_mem_free = "{}G".format(fmem),
                    max_runtime_seconds = seconds,
                    queue = "{}.q".format(queue),
                    j_resource = archive,
                    max_attempts = max_attempts,
                    upstream_tasks = upstream_tasks)

# ------------------------------------------------------------------------------

def generate_workflow(config):

    # create workflow object
    wf = Workflow("covid_em_data_{}".format(config['run_id_covid_em_data']),
			      project = config['submission_project_name'],
			      stderr = "FILEPATH".format(config['log_dir']),
			      stdout = "FILEPATH".format(config['log_dir']),
			      resume = True)

    # get location hierarchy
    loc_table = pd.read_csv("FILEPATH".format(config['main_dir']))
    loc_table = loc_table[(loc_table["is_estimate_1"]==True) | (loc_table["is_estimate_2"]==True) | (loc_table["ihme_loc_id"]=="Mumbai")]
    loc_list = loc_table.location_id.unique()

    # get external locations from config
    external_locs  = config['external_em_locs']

    # (1) DOWNLOAD INPUTS
    inputs_task = generate_task(
        jobname = "download_covid_em_data_inputs_{}".format(config['run_id_covid_em_data']),
        code = "FILEPATH".format(config['code_dir']),
        shell = config['shell_path'],
        args_shell = "-i {}".format(config['image_path']),
        args = "--main_dir {}".format(config['main_dir']),
        queue = config['queue'],
        fthread = 2, fmem = 25,
        archive = True,
        h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0)
    )
    wf.add_task(inputs_task)

    # (2) DATA PROCESSING - raw data for model by location
    processing_tasks = {}
    for loc in loc_list:
        ihme_loc = loc_table[loc_table['location_id'] == loc]['ihme_loc_id'].values[0]
        processing_tasks[loc] = generate_task(
            jobname = "process_covid_em_{}_{}".format(ihme_loc, config['run_id_covid_em_data']),
            code = "FILEPATH".format(config['code_dir']),
            shell = config['shell_path'],
            args_shell = "-i {}".format(config['image_path']),
            args = "--main_dir {} --loc_id {}".format(config['main_dir'], loc),
            queue = config['queue'],
            fthread = 5, fmem = 25,
            h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0),
            upstream_tasks = [inputs_task]
        )
        wf.add_task(processing_tasks[loc])

    # (3) DIAGNOSTICS
    diagnostics_task = generate_task(
        jobname = "diagnostics_covid_em_data_{}".format(config['run_id_covid_em_data']),
        code = "FILEPATH".format(config['code_dir']),
        shell = config['shell_path'],
        args_shell = "-i {}".format(config['image_path']),
        args = "--main_dir {}".format(config['main_dir']),
        queue = config['queue'],
        fthread = 2, fmem = 50,
        h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0),
        upstream_tasks = processing_tasks.values()
    )
    wf.add_task(diagnostics_task)

    return(wf)

# ------------------------------------------------------------------------------

def launch_workflow(wf, config):
    """
    Launch workflow, collect status, slack upon failure
    """
    success = wf.run()
    if success == 0:
        print("Completed!")
    else:
        raise Exception("An issue occured.")

# ------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--main_dir', type = str, required = True,
                        help = "main versioned directory for this data run")

    args = parser.parse_args()
    main_dir = args.main_dir

    # load config file
    with open("FILEPATH".format(main_dir)) as file:
        config = yaml.load(file, Loader = yaml.FullLoader)
        config = config.get("default")

    # generate and launch jobmon workflow
    wf = generate_workflow(config)
    launch_workflow(wf, config)

if __name__ == '__main__' :
    main()


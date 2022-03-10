
"""
Description: Submit the covid excess mortality process jobs using Jobmon
Steps:
    * Build jobmon workflow object with correct tasks
    * Submit workflow
    * Slack upon failure
Inputs:
    * {main_dir}/covid_em_detailed.yml: detailed configuration file
    * {main_dir}/process_locations.csv: locations to be submitted for parallel steps
"""

import argparse
import sys
import jobmon
import getpass
import pandas as pd
import datetime
import yaml

# load in functions from jobmon package
from jobmon import UnknownWorkflow as Workflow
from jobmon import BashTask

# set environment variable
import os
os.environ["SGE_ROOT"] = "SGE_PATH"


# ------------------------------------------------------------------------------


def generate_task(jobname, code, shell, args, fthread, fmem, h_rt,
                  args_shell = "", queue = "all", archive = False,
                  max_attempts = 3, upstream_tasks = []):

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
    wf = Workflow("covid_em_{}".format(config['run_id_covid_em_estimate']),
			      project = config['submission_project_name'],
			      stderr = "{}/errors/".format(config['log_dir']),
			      stdout = "{}/output/".format(config['log_dir']),
			      resume = True)

    # get location hierarchy
    loc_table = pd.read_csv("{}/inputs/process_locations.csv".format(config['main_dir']))
    loc_table = loc_table[loc_table["is_estimate_1"]==True]
    loc_list = loc_table.location_id.unique()
    
    # get sex groupings
    sex_table = pd.read_csv("{}/inputs/process_sexes.csv".format(config['main_dir']))
    sex_table = sex_table[sex_table["is_estimate"]==True]
    sex_list = sex_table.sex.unique()

    # (1) DOWNLOAD INPUTS
    inputs_task_a = generate_task(
        jobname = "download_covid_em_model_inputs_{}".format(config['run_id_covid_em_estimate']),
        code = "{}/01a_download_model_inputs.R".format(config['code_dir']),
        shell = config['shell_path'],
        args_shell = "-i {}".format(config['image_path']),
        args = "--main_dir {}".format(config['main_dir']),
        queue = config['queue'],
        fthread = 2, fmem = 20,
        h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0)
    )
    wf.add_task(inputs_task_a)

    inputs_task_b = generate_task(
        jobname = "download_covid_em_other_inputs_{}".format(config['run_id_covid_em_estimate']),
        code = "{}/01b_download_other_inputs.R".format(config['code_dir']),
        shell = config['shell_path'],
        args_shell = "-i {}".format(config['image_path']),
        args = "--main_dir {}".format(config['main_dir']),
        queue = config['queue'],
        fthread = 2, fmem = 20,
        h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0)
    )
    wf.add_task(inputs_task_b)

    # (2) FIT MODEL
    run_model_types = config['run_model_types']
    if not isinstance(run_model_types, list): run_model_types = [run_model_types]
    if "poisson" in run_model_types:
        fit_tasks = {}
        for loc in loc_list:
            ihme_loc = loc_table[loc_table['location_id'] == loc]['ihme_loc_id'].values[0]
            fit_tasks[loc] = generate_task(
                jobname = "fit_covid_em_{}_{}".format(ihme_loc, config['run_id_covid_em_estimate']),
                code = "{}/02_fit_model.R".format(config['code_dir']),
                shell = config['shell_path'],
                args_shell = "-i {}".format(config['image_path']),
                args = "--main_dir {} --loc_id {}".format(config['main_dir'], loc),
                queue = config['queue'],
                fthread = 3, fmem = 120,
                h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0),
                upstream_tasks = [inputs_task_a]
            )
            wf.add_task(fit_tasks[loc])

    # regmod model fit for every tail size specified
    if "regmod" in run_model_types:
        tail_size_month = config['tail_size_month']
        if not isinstance(tail_size_month, list): tail_size_month = [tail_size_month]
        fit_regmod_tasks = {}
        for ts in tail_size_month:
            ts = int(ts)
            prep_meta_task = generate_task(
                jobname = "prep_meta_{}_{}".format(ts, config['run_id_covid_em_estimate']),
                code = "{}/01c_prep_meta.R".format(config['code_dir']),
                shell = config['shell_path'],
                args_shell = "-i {}".format(config['image_path']),
                args = "--main_dir {} --ts {}".format(config['main_dir'], ts),
                queue = config['queue'],
                fthread = 2, fmem = 10,
                h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0),
                upstream_tasks = [inputs_task_a]
            )
            wf.add_task(prep_meta_task)
        
            for loc in loc_list:
                ihme_loc = loc_table[loc_table['location_id'] == loc]['ihme_loc_id'].values[0]
                fit_regmod_tasks[loc] = generate_task(
                    jobname = "fit_covid_em_regmod_{}_{}_{}".format(ihme_loc, ts, config['run_id_covid_em_estimate']),
                    code = "{}/02b_fit_regmod_model.py".format(config['code_dir']),
                    shell = "{}/regmod_shell.sh".format(config['code_dir']),
                    args = "--main_dir {} --ihme_loc {} --ts {}".format(config['main_dir'], ihme_loc, ts),
                    # args_shell = "-i {} -l python3".format(config['image_path']),
                    queue = config['queue'],
                    fthread = 2, fmem = 30,
                    h_rt = datetime.timedelta(days = 1, hours = 0, minutes = 0),
                    upstream_tasks = [prep_meta_task]
                )
                wf.add_task(fit_regmod_tasks[loc])

    # (3) COMPLETE STAGE-1
    complete_stage1_tasks = {}
    upstream_tasks = []
    if "poisson" in run_model_types:
        upstream_tasks = upstream_tasks + [fit_tasks[loc]]
    if "regmod" in run_model_types:
        upstream_tasks = upstream_tasks + [fit_regmod_tasks[loc]]
    for loc in loc_list:
        ihme_loc = loc_table[loc_table['location_id'] == loc]['ihme_loc_id'].values[0]
        complete_stage1_tasks[loc] = generate_task(
            jobname = "complete_stage1_covid_em_{}_{}".format(ihme_loc, config['run_id_covid_em_estimate']),
            code = "{}/03_complete_stage1.R".format(config['code_dir']),
            shell = config['shell_path'],
            args_shell = "-i {}".format(config['image_path']),
            args = "--main_dir {} --loc_id {}".format(config['main_dir'], loc),
            queue = config['queue'],
            fthread = 5, fmem = 80,
            h_rt = datetime.timedelta(days = 0, hours = 3, minutes = 0),
            upstream_tasks = upstream_tasks
        )
        wf.add_task(complete_stage1_tasks[loc])

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
                        help = "main versioned directory for this model run")

    args = parser.parse_args()
    main_dir = args.main_dir

    # load config file
    with open("{}/covid_em_detailed.yml".format(main_dir)) as file:
        config = yaml.load(file, Loader = yaml.FullLoader)
        config = config.get("default")

    # generate and launch jobmon workflow
    wf = generate_workflow(config)
    launch_workflow(wf, config)

if __name__ == '__main__' :
    main()


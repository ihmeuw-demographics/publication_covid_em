
# Meta --------------------------------------------------------------------

# Description: Sets up a new COVID-19 excess mortality estimate run.
# Steps:
#   1. Reads in and checks config files.
#   2. Creates a new version number.
#   3. Creates folder structure for new run.
#   4. Outputs detailed config file for later steps.
#   5. submit script `00b_submit_run.R`.
# Inputs:
#   * {config_dir}/all.yml: configuration file for all demographic processes.
#   * {config_dir}/covid_em.yml: configuration file for COVID-19 excess mortality
#         process.
# Outputs:
#   * {main_dir}/covid_em_detailed.yml: detailed configuration file that is
#     run-specific.
#     - Includes the same variables as in the main configuration file but named
#       input run_ids are filled in with corresponding numbers.
#     - Newly created variables like {run_id_covid_em_estimate},
#       {main_dir}, etc. are included

# Load libraries ----------------------------------------------------------

library(argparse)
library(assertthat)
library(data.table)
library(demInternal)
library(fs)
library(yaml)


# Command line arguments --------------------------------------------------

# interactive testing instructions:
#  - default arguments will be used
#  - set `repo_dir_default` to where you cloned the `configs` and
#    `model_covid_em` repos, but do not commit this change
repo_dir_default <- fs::path("FILEPATH", Sys.getenv("USER"))

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--code_dir", type = "character",
  required = !interactive(),
  default = fs::path(repo_dir_default, "model_covid_em"),
  help = "path to directory containing code files"
)
parser$add_argument(
  "--config_dir", type = "character",
  required = !interactive(),
  default = fs::path(repo_dir_default, "configs"),
  help = "path to directory with `all.yml` and `covid_em.yml` config files"
)
parser$add_argument(
  "--configuration_name_all", type = "character",
  required = !interactive(), default = "default",
  help = "configuration from `all.yml` to use for this process run"
)
parser$add_argument(
  "--configuration_name_process", type = "character",
  required = !interactive(), default = "default",
  help = "configuration from `covid_em.yml` to use for this process run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# assertions for command line arguments
assertthat::assert_that(fs::dir_exists(code_dir))
assertthat::assert_that(fs::dir_exists(config_dir))


# Load configuration files ------------------------------------------------

# load all demographics config file
config_all <- config::get(
  file = fs::path(config_dir, "all.yml"),
  config = configuration_name_all,
  use_parent = FALSE
)
list2env(config_all, .GlobalEnv)

# load COVID excess mortality specific config file
config_covid_em <- config::get(
  file = fs::path(config_dir, "covid_em.yml"),
  config = configuration_name_process,
  use_parent = FALSE
)
list2env(config_covid_em, .GlobalEnv)

# determine actual run ids for named run ids and external input ids
# (like "best" or "recent")
config_covid_em <- demInternal::prep_config_run_ids(
  config = config_covid_em,
  gbd_year = gbd_year,
  decomp_step = decomp_step,
  odbc_dir = odbc_dir,
  odbc_section = odbc_section
)
list2env(config_covid_em, .GlobalEnv)


# Create version name -----------------------------------------------------

# get current time in "Year-Month-Day-Hour-Minute" format
run_id_covid_em_estimate <- format(Sys.time(), "%Y-%m-%d-%H-%M")


# Create folder structure -------------------------------------------------

main_dir <- fs::path(base_dir, run_id_covid_em_estimate)

dir.create(main_dir)
dir.create(fs::path(main_dir, "inputs"))
dir.create(fs::path(main_dir, "outputs"))
dir.create(fs::path(main_dir, "diagnostics"))
dir.create(fs::path(main_dir, "diagnostics/location_specific"))

dir.create(fs::path(main_dir, "outputs/fit"))
dir.create(fs::path(main_dir, "outputs/draws"))
dir.create(fs::path(main_dir, "outputs/summaries"))
dir.create(fs::path(main_dir, "outputs/summaries_regmod"))
dir.create(fs::path(main_dir, "outputs/summaries_stage2"))


# Create detailed config file ---------------------------------------------

# save original config files
fs::file_copy(
  path = fs::path(config_dir, "all.yml"),
  new_path = fs::path(main_dir, "all.yml")
)
fs::file_copy(
  path = fs::path(config_dir, "covid_em.yml"),
  new_path = fs::path(main_dir, "covid_em.yml")
)

# merge together config objects
config_detailed <- data.table::copy(config_all)
config_detailed <- config::merge(config_detailed, config_covid_em)

# add on other important created variables
add_config_vars <- c(
  "run_id_covid_em_estimate",
  "main_dir",
  "code_dir",
  "config_dir",
  "configuration_name_all",
  "configuration_name_process"
)
for (var in add_config_vars) {
  config_detailed[[var]] <- get(var)
}

# save detailed config file
config_detailed <- list(default = config_detailed)
yaml::write_yaml(config_detailed, fs::path(main_dir, "covid_em_detailed.yml"))


# Mappings ----------------------------------------------------------------

# location
process_locations <- demInternal::location_mapping
process_locations <- process_locations[
  , list(location_id, parent_location_id, ihme_loc_id, parent_country,
          estimation_type = get("estimation_type"))
]
process_locations[, is_estimate_1 := grepl("^estimate", estimation_type)]
process_locations[, is_estimate_2 := grepl("^estimate", estimation_type)]

# only include locations with all-cause data
data_locations <- fread(
  fs::path(base_dir_data, external_id_covid_em_data, "inputs/process_locations.csv")
)[(is_estimate_1), ihme_loc_id]
data_locations <- setdiff(data_locations, holdout_locations)
process_locations[!ihme_loc_id %in% data_locations, is_estimate_1 := F]
if (test_subset) {
  process_locations[!ihme_loc_id %in% subset_locations,
                    `:=` (is_estimate_1 = F, is_estimate_2 = F)]
}
# can't fit model w/o data before the first covid year
if (2019 %in% covid_years) {
  process_locations[ihme_loc_id %in% locs_start_at_2019, is_estimate_1 := F]
}
readr::write_csv(
  x = process_locations,
  file = fs::path(main_dir, "/inputs/process_locations.csv")
)

# sex
process_sexes <- demInternal::sex_mapping
process_sexes <- process_sexes[, list(sex, sex_id, parent_sex, estimation_type)]
process_sexes <- process_sexes[estimation_type != "not_estimated"]
process_sexes[sex == "all", estimation_type := "estimated"]
process_sexes[, is_estimate := grepl("^estimate", estimation_type)]
readr::write_csv(
  x = process_sexes,
  file = fs::path(main_dir, "/inputs/process_sexes.csv")
)

# age
process_ages <- demInternal::age_mapping
process_ages <- process_ages[
  , list(age_start, age_end, age_group_id, estimation_type = lifetable)
]

process_ages[age_start >= 95, estimation_type := NA]
process_ages[age_start == 95 & is.infinite(age_end), estimation_type := "estimated"]
process_ages <- process_ages[estimation_type == "estimated" & age_start >= 5]
process_ages <- rbind(
  process_ages,
  data.table(age_start = c(0, 0), age_end = c(5, 125), age_group_id = c(1, 22),
             estimation_type = "estimated")
)
process_ages[is.infinite(age_end), age_end := 125]
process_ages[, is_estimate := grepl("^estimate", estimation_type)]
readr::write_csv(
  x = process_ages,
  file = fs::path(main_dir, "/inputs/process_ages.csv")
)


# Submit job submission job -----------------------------------------------

demInternal::qsub(
  jobname = paste0("submit_covid_em_", run_id_covid_em_estimate),
  shell = fs::path(code_dir, "python_shell.sh"),
  code = fs::path(code_dir, "00b_submit_run.py"),
  fthread = 1, fmem = 1, h_rt = "02:00:00:00", archive = F,
  pass_argparse = list(main_dir = main_dir),
  queue = queue,
  proj = submission_project_name,
  submit = TRUE
)

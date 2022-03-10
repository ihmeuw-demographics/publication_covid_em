
# Meta --------------------------------------------------------------------

# Description: Sets up a new COVID-19 excess mortality data run.
# Steps:
#   1. Reads in and checks config files.
#   2. Creates a new version number.
#   3. Creates folder structure for new run.
#   4. Outputs detailed config file for later steps.
#   5. qsubs the submission script `00b_submit_run.R`.
#   7. Slacks update if run setup and submission is successful.


# Load libraries ----------------------------------------------------------

  message(Sys.time(), " | Setup")

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
#    `processing_covid_em` repos, but do not commit this change
repo_dir_default <- fs::path("FILEPATH", Sys.getenv("USER"))

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--code_dir", type = "character",
  required = !interactive(),
  default = fs::path(repo_dir_default, "processing_covid_em"),
  help = "path to directory containing code files"
)
parser$add_argument(
  "--config_dir", type = "character",
  required = !interactive(),
  default = fs::path(repo_dir_default, "configs"),
  help = "path to directory with `all.yml` and `covid_em_data.yml` config files"
)
parser$add_argument(
  "--configuration_name_all", type = "character",
  required = !interactive(), default = "test_production",
  help = "configuration from `all.yml` to use for this process run"
)
parser$add_argument(
  "--configuration_name_process", type = "character",
  required = !interactive(), default = "default",
  help = "configuration from `covid_em_data.yml` to use for this process run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# assertions for command line arguments
assertthat::assert_that(fs::dir_exists(code_dir))
assertthat::assert_that(fs::dir_exists(config_dir))


# Load configuration files ------------------------------------------------

message(Sys.time(), " | Load configs")

# load all demographics config file
config_all <- config::get(
  file = fs::path(config_dir, "all.yml"),
  config = configuration_name_all,
  use_parent = FALSE
)
list2env(config_all, .GlobalEnv)

# load COVID excess mortality data specific config file
config_covid_em <- config::get(
  file = fs::path(config_dir, "covid_em_data.yml"),
  config = configuration_name_process,
  use_parent = FALSE
)
list2env(config_covid_em, .GlobalEnv)

# determine actual run ids for named run ids and external input ids
# (like "best" or "recent")
config_covid_em <- demInternal::prep_config_run_ids(
  config = config_covid_em,
  gbd_year = gbd_year,
  odbc_dir = odbc_dir,
  odbc_section = odbc_section
)
list2env(config_covid_em, .GlobalEnv)


# Create version name -----------------------------------------------------

message(Sys.time(), " | Create a version")

# get current time in "Year-Month-Day-Hour-Minute" format
run_id_covid_em_data <- format(Sys.time(), "%Y-%m-%d-%H-%M")


# Create folder structure -------------------------------------------------

message(Sys.time(), " | Create folders")

main_dir <- fs::path(
  base_dir, run_id_covid_em_data, "/"
)
main_dir

fs::dir_create(main_dir, mode = "u=rwx,go=rwx")
fs::dir_create(fs::path(main_dir, "inputs"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path(main_dir, "outputs"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path(main_dir, "outputs", "external"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path(main_dir, "replaced_w_wmd"), mode = "u=rwx,go=rwx")
fs::dir_create(fs::path(main_dir, "diagnostics"), mode = "u=rwx,go=rwx")


# Create detailed config file ---------------------------------------------

message(Sys.time(), " | Save detailed config")

# save original config files
fs::file_copy(
  path = fs::path(config_dir, "all.yml"),
  new_path = fs::path(main_dir, "all.yml")
)
fs::file_copy(
  path = fs::path(config_dir, "covid_em_data.yml"),
  new_path = fs::path(main_dir, "covid_em_data.yml")
)

# merge together config objects
config_detailed <- data.table::copy(config_all)
config_detailed <- config::merge(config_detailed, config_covid_em)

# add on other important created variables
add_config_vars <- c(
  "run_id_covid_em_data",
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
yaml::write_yaml(config_detailed, fs::path(main_dir, "covid_em_data_detailed.yml"))


# Mappings ----------------------------------------------------------------

message(Sys.time(), " | Save mappings")

message(Sys.time(), " | ... Locations")

# location
process_locations <- demInternal::location_mapping #pulls mortality computation dataset
process_locations <- process_locations[
  , list(location_id, parent_location_id, ihme_loc_id, parent_country,
         estimation_type = get("estimation_type"))
]
process_locations[, is_estimate_1 := grepl("^estimate", estimation_type)] # stage 1
process_locations[, is_estimate_2 := grepl("^estimate", estimation_type)] # stage 2
# add covid specific subnationals
covid_locs <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")
covid_locs <- covid_locs[parent_id %in% c(570, 101, 92, 81),
                         c("location_id","parent_id","ihme_loc_id")]
setnames(covid_locs, "parent_id","parent_location_id")
covid_locs[!parent_location_id==570, parent_country := substr(ihme_loc_id, 1, 3)]
covid_locs[parent_location_id==570, parent_country := "USA_570"]
covid_locs[, ":=" (estimation_type = "estimated", is_estimate_1 = F, is_estimate_2 = T)]
process_locations <- rbind(process_locations,covid_locs)
data_locations <- unique(c(
  list.dirs(all_cause_data_dir, full.names = F, recursive = F),
  list.dirs(all_cause_data_dir_lu, full.names = F, recursive = F)
))

data_locations <- setdiff(data_locations, holdout_locations)

process_locations[!ihme_loc_id %in% data_locations, is_estimate_1 := 0]
if (test_subset) {
  process_locations[
    !ihme_loc_id %in% subset_locations,
    `:=` (is_estimate_1 = 0, is_estimate_2 = 0)
  ]
}
readr::write_csv(
  x = process_locations,
  file = fs::path(main_dir, "FILEPATH")
)

message(Sys.time(), " | ... Sexes")

# sex
process_sexes <- demInternal::sex_mapping
process_sexes <- process_sexes[, list(sex, sex_id, parent_sex, estimation_type)]
process_sexes <- process_sexes[estimation_type != "not_estimated"]
process_sexes[sex == "all", estimation_type := "estimated"]
process_sexes[, is_estimate := grepl("^estimate", estimation_type)]
readr::write_csv(
  x = process_sexes,
  file = fs::path(main_dir, "FILEPATH")
)

message(Sys.time(), " | ... Ages")

# age
process_ages <- demInternal::age_mapping
process_ages <- process_ages[
  , list(age_start, age_end, age_group_id, estimation_type = lifetable)
  ]
process_ages[age_start >= 95, estimation_type := NA]
process_ages[age_start == 95 & is.infinite(age_end), estimation_type := "estimated"]
# use 0-5 rather than 0-1 and 1-5
process_ages[age_group_id==1, estimation_type := "estimated"]
process_ages[age_group_id %in% c(28,5), estimation_type := NA]
process_ages <- process_ages[estimation_type == "estimated"]
process_ages <- rbind(
  process_ages,
  data.table(age_start = 0, age_end = 125, age_group_id = 22, estimation_type = "estimated")
)
process_ages[is.infinite(age_end), age_end := 125]
process_ages[, is_estimate := grepl("^estimate", estimation_type)]
readr::write_csv(
  x = process_ages,
  file = fs::path(main_dir, "FILEPATH")
)


# Submit job submission job -----------------------------------------------

message(Sys.time(), " | Submit jobmon job")

demInternal::qsub(
  jobname = paste0("submit_covid_em_data_", run_id_covid_em_data),
  shell = fs::path(code_dir, "python_shell.sh"),
  code = fs::path(code_dir, "00b_submit_run.py"),
  fthread = 1, fmem = 1, h_rt = "02:00:00:00", archive = F,
  pass_argparse = list(main_dir = main_dir),
  queue = queue,
  proj = submission_project_name,
  submit = TRUE
)


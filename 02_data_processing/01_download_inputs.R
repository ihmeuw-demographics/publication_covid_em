
# Meta --------------------------------------------------------------------

# Description: Downloads/compiles all inputs needed to run COVID excess
#  mortality data processing


# Load libraries ----------------------------------------------------------

message(Sys.time(), " | Setup")

library(argparse)
library(arrow)
library(assertable)
library(aweek)
library(data.table)
library(demInternal)
library(dplyr)
library(magrittr)
library(yaml)
library(zoo)


# Command line arguments --------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup -------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(main_dir, "FILEPATH"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# load in custom functions
source(paste0(code_dir, "FILEPATH"))
source(paste0(code_dir, "FILEPATH"))

# get mappings
process_locations <- fread(paste0(main_dir, "FILEPATH"))
process_locations <- process_locations[!ihme_loc_id == "Mumbai"]
process_locations$location_id <- as.numeric(process_locations$location_id)
process_sexes <- fread(paste0(main_dir, "FILEPATH"))
process_ages <- fread(paste0(main_dir, "FILEPATH"))
covid_loc_map <- demInternal::get_locations(gbd_year = gbd_year, location_set_name = "COVID-19 modeling")

# source prioritization
source_prioritization <- fread(paste0(code_dir, "FILEPATH"))

# location week types
location_week_types <- fread(fs::path(code_dir,"FILEPATH"))
locs_with_isoweek <- location_week_types[week_type == "isoweek", ihme_loc_id]
locs_with_epiweek <- location_week_types[week_type == "epiweek", ihme_loc_id]
locs_with_ukweek <- location_week_types[week_type == "ukweek", ihme_loc_id]


# All-cause mortality data -----------------------------------------------------

message(Sys.time(), " | All-cause data")

allcause <- load_recent(
  ihme_loc_list = process_locations[(is_estimate_1), ihme_loc_id],
  dir_list = c(all_cause_data_dir, all_cause_data_dir_lu),
  source_prioritization = source_prioritization
)

# format
allcause <- allcause[sex_id %in% c(1:3)]
allcause <- demInternal::ids_names(allcause, id_cols = "sex_id")
cols <- c("nid", "underlying_nid", "source", "source_type", "ihme_loc_id",
          "location_note", "year_start", "week_start", "week_end", "month_start",
          "month_end", "day_start", "day_end", "sex", "age_start", "age_end",
          "date_reported", "cause_id", "deaths", "date_prepped")
allcause <- allcause[, .SD, .SDcols = cols]
allcause <- allcause[!is.na(age_start) & age_start < 150]
allcause[, deaths := gsub(",", "", deaths)]
allcause[, deaths := as.numeric(deaths)]
allcause <- allcause[!is.na(deaths)]
allcause <- allcause[year_start %in% estimation_year_start:estimation_year_end]

# check
missing_locations <- setdiff(
  process_locations[(is_estimate_1), ihme_loc_id],
  unique(allcause$ihme_loc_id)
)
assertthat::assert_that(
  length(missing_locations) == 0,
  msg = paste0("Missing data or import error for these locations: ",
               paste(missing_locations, collapse = ", "))
)
assertable::assert_colnames(allcause, cols, quiet = T)
assertable::assert_values(allcause, "deaths", test = "gte", test_val = 0, quiet = T)
assertable::assert_values(allcause, "cause_id", test = "equal", test_val = 294, quiet = T)

# save
readr::write_csv(
  allcause, paste0(main_dir, "FILEPATH")
)
rm(allcause)


# External em data -------------------------------------------------------------

message(Sys.time(), " | External em data")

ext_em <- load_recent(
  ihme_loc_list = external_em_locs,
  dir_list = c(external_em_dir, external_em_dir_lu)
)

# check
missing_locations <- setdiff(
  external_em_locs,
  unique(ext_em$ihme_loc_id)
)
assertthat::assert_that(
  length(missing_locations) == 0,
  msg = paste0("Missing data or import error for these locations: ",
               paste(missing_locations, collapse = ", "))
)

assertable::assert_values(
  ext_em, c(setdiff(id_cols, "location_id"),"location_name","model_type","source",
           "deaths_excess"), test = "not_na", quiet = T
)
assertable::assert_values(ext_em, "cause_id", test = "equal", test_val = 294, quiet = T)

# save
readr::write_csv(
  ext_em, paste0(main_dir, "FILEPATH")
)
rm(ext_em)


# WMD all-cause mortality data --------------------------------------------

message(Sys.time(), " | WMD")

wmd <- fread(wmd_file)

colnames(wmd) <- c("location_name", "year_start", "time_start", "time_unit", "deaths")

wmd <- wmd[time_unit %in% c("weekly", "monthly")]

wmd[, time_unit := gsub("ly", "", time_unit)]

wmd[location_name == "Bolivia", location_name := "Bolivia (Plurinational State of)"]
wmd[location_name == "Bosnia", location_name := "Bosnia and Herzegovina"]
wmd[location_name == "Brunei", location_name := "Brunei Darussalam"]
wmd[location_name == "Hong Kong", location_name := "Hong Kong Special Administrative Region of China"]
wmd[location_name == "Iran", location_name := "Iran (Islamic Republic of)"]
wmd[location_name == "Macao", location_name := "Macao Special Administrative Region of China"]
wmd[location_name == "Moldova", location_name := "Republic of Moldova"]
wmd[location_name == "Russia", location_name := "Russian Federation"]
wmd[location_name == "South Korea", location_name := "Republic of Korea"]
wmd[location_name == "Taiwan", location_name := "Taiwan (Province of China)"]
wmd[location_name == "United States", location_name := "United States of America"]
wmd[location_name == "Venezuela", location_name := "Venezuela (Bolivarian Republic of)"]

wmd <- wmd[!(location_name %in% c("Aruba", "Faroe Islands", "French Guiana",
                                            "French Polynesia", "Gibraltar", "Guadeloupe",
                                            "Kosovo", "Liechtenstein", "Martinique",
                                            "Mayotte", "New Caledonia", "R\xe9union", "Transnistria"))]

wmd <- merge(
  wmd,
  covid_loc_map[level == 3 | location_id %in% c(354, 361), c("location_name", "location_id")],
  by = "location_name",
  all.x = T
)

wmd[location_name == "Hong Kong Special Administrative Region of China", location_id := 354]
wmd[location_name == "Macao Special Administrative Region of China", location_id := 361]

assertable::assert_values(wmd, "location_id", test = "not_na")

wmd[, location_name := NULL]
wmd[, sex := "all"]
wmd[, age_start := 0]
wmd[, age_end := 125]
wmd[, source := NA]

# subset to needed cols
keep <- c("year_start", "location_id", "sex", "time_start", "time_unit",
          "age_start", "age_end", "source", "deaths")
wmd <- wmd[, ..keep]

# save
readr::write_csv(
  wmd, paste0(main_dir, "FILEPATH")
)
rm(wmd)

# COVID death data --------------------------------------------------------

message(Sys.time(), " | COVID data: ", covid_file)

# deal with subnational units present in covid file
esp_loc_ids <- unique(covid_loc_map[parent_id==92, location_id])
ita_loc_ids <- unique(covid_loc_map[parent_id==86, location_id])
deu_loc_ids <- unique(covid_loc_map[parent_id==81, location_id])
can_loc_ids <- unique(covid_loc_map[parent_id==101, location_id])
ind_loc_ids <- unique(covid_loc_map[parent_id==163, location_id])
bra_loc_ids <- unique(covid_loc_map[parent_id==135, location_id])
mex_loc_ids <- unique(covid_loc_map[parent_id==130, location_id])
pak_loc_ids <- unique(covid_loc_map[parent_id==165, location_id])

# load data
covid_raw <- fread(covid_file)

# Fill dates between min and max for each location
covid_raw <- covid_raw[!is.na(Deaths)]
setnames(covid_raw, "Date", "date")
covid <- fill_missing_dates(covid_raw)

# drop these included nat location without deaths
covid <- covid[!location_id %in% c(81, 130, 95)]

# check squareness
covid <- check_square(covid)

# add ihme loc id
covid <- merge(
  covid,
  process_locations[,c("location_id","ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)
unique(covid[is.na(ihme_loc_id), "location_name"])

# deaths are cumulative -- find weekly
# first interpolate NAs, assuming zero additional deaths until the next
# non-NA cumulative death count is reported
covid <- covid[order(date)]
covid <- covid %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(Deaths, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

# get daily deaths then sum to weekly
covid[, daily_deaths := Deaths - shift(Deaths), by = "location_id"]

# get week/year from date based on country week type
covid_w <- copy(covid)
covid_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek), ':=' (time_start = lubridate::week(date),
                                                                       year_start = lubridate::year(date))]
covid_w[!ihme_loc_id %in% c(locs_with_isoweek, locs_with_epiweek, locs_with_ukweek) & time_start == 53, time_start := 52]
covid_w[ihme_loc_id %in% locs_with_isoweek, ':=' (time_start = lubridate::isoweek(date),
                                                year_start = lubridate::isoyear(date))]
covid_w[ihme_loc_id %in% locs_with_epiweek, ':=' (time_start = lubridate::epiweek(date),
                                                year_start = lubridate::epiyear(date))]
covid_w[ihme_loc_id %in% locs_with_ukweek, ':=' (week_year = aweek::date2week(date, week_start = "sat", floor_day = T))]
covid_w[ihme_loc_id %in% locs_with_ukweek, ':=' (time_start = substr(week_year,7,8),
                                               year_start = substr(week_year,1,4))]
covid_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2020, ':=' (time_start = time_start + 1)]
covid_w[ihme_loc_id %in% locs_with_ukweek & year_start == 2019 & time_start == 53, ':=' (time_start = 1,
                                                                                         year_start = 2020)]
covid_w[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]
covid_w[, ':=' (week_year = NULL, time_unit = "week")]

# get month/year from date
covid_m <- copy(covid)
covid_m[, ':=' (time_start = lubridate::month(date), year_start = lubridate::year(date), time_unit = "month")]
covid_m[, c("time_start", "year_start") := lapply(.SD, as.numeric), .SDcols = c("time_start", "year_start")]

# combine time series
covid <- rbind(covid_w, covid_m)

# aggregate over time unit
covid <- covid[,
  list(deaths_covid = sum(daily_deaths, na.rm = TRUE)),
  by = c("year_start", "time_start", "time_unit", "location_id")
]
covid <- covid[!is.na(deaths_covid)]

# sum USA over subnational units
wa_state_loc_ids <- c(60886, 3539, 60887)
usa_loc_ids <- c(
  process_locations[ihme_loc_id %like% "USA", location_id],
  wa_state_loc_ids
)
assertthat::assert_that(!102 %in% unique(covid$location_id))
covid_usa_agg <- covid[location_id %in% usa_loc_ids]
covid_usa_agg[, location_id := 102]
covid_wa_agg <- covid[location_id %in% wa_state_loc_ids]
covid_wa_agg[, location_id := 570]
covid <- rbindlist(list(covid, covid_usa_agg, covid_wa_agg), use.names = T)

# change ihme_loc_id to national so that when we aggregate over ihme_loc_id
# we get national sum
change_subnat_to_nat <- function(national_ihme_loc, covid) {
  print(national_ihme_loc)
  national <- copy(covid)
  if(national_ihme_loc == "CHN"){
    national_loc_id <- 6
  } else {
    national_loc_id <- process_locations[ihme_loc_id == national_ihme_loc, location_id]
    national_loc_id <- as.numeric(national_loc_id)
  }
  assertthat::assert_that(!national_loc_id %in% unique(covid$location_id))
  national <- national[location_id %in% get(paste0(tolower(national_ihme_loc), "_loc_ids"))]
  national[, location_id := national_loc_id]
  return(national)
}
covid_nats <- rbindlist(lapply(
  c("ITA","ESP","DEU","CAN","IND","BRA","MEX","PAK"),
  change_subnat_to_nat,
  covid = covid
))
covid <- rbind(covid, covid_nats)

# special process for GBR:
# 1. remove United Kingdom total
# 2. combine England (4749) and Wales (4636) to match HMD all-cause locs (make GBR_432)
gbr_national <- copy(covid)
gbr_national <- gbr_national[location_id %in% c(4749, 4636)]
gbr_national[location_id %in% c(4749, 4636), location_id := 432]
covid <- rbind(covid, gbr_national)

# special process for ITA subnats:
# ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
ita_combine <- copy(covid)
ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
ita_combine[, location_id := 99999]
covid <- rbind(covid, ita_combine)

# aggregate
covid <- covid[,
  list(deaths_covid = sum(deaths_covid)),
  by = c("year_start", "time_start", "time_unit", "location_id")
]

# add extra variables
covid[, age_start := 0]
covid[, age_end := 125]
covid[, sex := "all"]
covid[, date_prepped := gsub("-", "_", Sys.Date())]
covid[, type := "covid_team"]

# types (for compatibility with `covid_custom` below)
num_cols <- c("location_id", "year_start", "time_start", "age_start",
              "age_end", "deaths_covid")
covid[, (num_cols) := lapply(.SD, as.numeric), .SDcols = num_cols]

# check
covid <- validate_download_data(
  dt = covid,
  process_locations = process_locations,
  not_na = names(covid)
)
assertable::assert_values(
  covid, "deaths_covid", test = "gte", test_val = 0, warn_only = T
)
covid[deaths_covid < 0, deaths_covid := 0]

# remove 2019 values
covid <- covid[(year_start %in% covid_years) | (deaths_covid >= 0)]

# save
readr::write_csv(covid, paste0(main_dir, "FILEPATH"))
rm(covid)


# Person Years -----------------------------------------------------------------

message(Sys.time(), " | Person Years (GBD)")

person_years <- fread("FILEPATH")

person_years[, ':=' (age_start = 0, age_end = 125)]
person_years <- person_years[, -c("age_name","ihme_loc_id")]
setnames(person_years,"type","pop_source")

# check
person_years <- validate_download_data(
  dt = person_years,
  process_locations = process_locations,
  not_na = names(person_years)
)
assertable::assert_values(
  person_years, "person_years", test = "gte", test_val = 0, warn_only = T
)

# save
readr::write_csv(person_years, paste0(main_dir, "FILEPATH"))
rm(person_years)


# IDR --------------------------------------------------------------------------

message(Sys.time(), " | IDR Covariate")

# Read in draws and format to mean values
idrcov <- setDT(read_parquet(idr_cov_file))
idrcov <- idrcov[!is.na(idr)]

idrcov <- idrcov[, .(idr = mean(idr, na.rm = T)),
           by = c("location_id","date")]
idrcov$date <- as.IDate(idrcov$date)

# checks
idrcov <- check_square(idrcov)
idrcov <- validate_download_data(
  dt = idrcov,
  process_locations = process_locations,
  not_na =  c("location_id","date","idr")
)
assertable::assert_values(idrcov, "idr", test = "gte", test_val = 0, quiet = T)

readr::write_csv(idrcov, paste0(main_dir, "FILEPATH"))
rm(idrcov)

idr_meta <- read_yaml(idr_meta_file)
write_yaml(idr_meta, paste0(main_dir, "FILEPATH"))


# Daily Covid Cases ------------------------------------------------------------

message(Sys.time(), " | Daily Covid Cases")

covid_daily_confirmed_raw <- fread(covid_file)
# Make dataset square
setnames(covid_daily_confirmed_raw, "Date", "date")
covid_daily_confirmed <- fill_missing_dates(covid_daily_confirmed_raw)
covid_daily_confirmed <- covid_daily_confirmed[order(location_id, date)]

# check square before shift and aggregation
covid_daily_confirmed <- check_square(covid_daily_confirmed)

# linear interpolate missing data
covid_daily_confirmed <- covid_daily_confirmed[, c("location_id","date","Confirmed")]
covid_daily_confirmed <- covid_daily_confirmed[order(location_id, date)]
covid_daily_confirmed <- do.call("rbind", by(covid_daily_confirmed, covid_daily_confirmed$location_id, na.trim, sides = "both"))
covid_daily_confirmed <- covid_daily_confirmed %>%
  group_by(location_id) %>%
  mutate(fill = na.approx(Confirmed)) %>%
  setDT()
covid_daily_confirmed[is.na(Confirmed), Confirmed := fill]

# calculate daily cases
covid_daily_confirmed[, daily_cases := Confirmed - shift(Confirmed, type = "lag"),
                      by = "location_id"]
covid_daily_confirmed[daily_cases < 0, daily_cases := 0]
covid_daily_confirmed[is.na(daily_cases), daily_cases := 0]

# Get national locations etc.
covid_daily_confirmed <- covid_daily_confirmed[!location_id %in% c(81, 130, 95)]

# sum USA over subnational units
wa_state_loc_ids <- c(60886, 3539, 60887)
usa_loc_ids <- c(
  process_locations[ihme_loc_id %like% "USA", location_id],
  wa_state_loc_ids
)
assertthat::assert_that(!102 %in% unique(covid_daily_confirmed$location_id))
covid_daily_confirmed_usa_agg <- covid_daily_confirmed[location_id %in% usa_loc_ids]
covid_daily_confirmed_usa_agg[, location_id := 102]
covid_daily_confirmed_wa_agg <- covid_daily_confirmed[location_id %in% wa_state_loc_ids]
covid_daily_confirmed_wa_agg[, location_id := 570]
covid_daily_confirmed <- rbindlist(list(covid_daily_confirmed, covid_daily_confirmed_usa_agg, covid_daily_confirmed_wa_agg), use.names = T)

change_subnat_to_nat <- function(national_ihme_loc, covid_daily_confirmed) {
  national <- copy(covid_daily_confirmed)
  if (national_ihme_loc == "CHN") {
    national_loc_id <- 6
  } else {
    national_loc_id <- process_locations[ihme_loc_id == national_ihme_loc, location_id]
  }
  assertthat::assert_that(!national_loc_id %in% unique(covid_daily_confirmed$location_id))
  national <- national[location_id %in% get(paste0(tolower(national_ihme_loc), "_loc_ids"))]
  national[, location_id := national_loc_id]
  return(national)
}
covid_daily_confirmed_nats <- rbindlist(lapply(
  c("ITA","ESP","DEU","CAN","IND","BRA","MEX","PAK"),
  change_subnat_to_nat,
  covid_daily_confirmed = covid_daily_confirmed
))
covid_daily_confirmed <- rbind(covid_daily_confirmed, covid_daily_confirmed_nats)

# special process for GBR:
# 1. remove United Kingdom total
# 2. combine England (4749) and Wales (4636) to match HMD all-cause locs (make GBR_432)
gbr_national <- copy(covid_daily_confirmed)
gbr_national <- gbr_national[location_id %in% c(4749, 4636)]
gbr_national[location_id %in% c(4749, 4636), location_id := 432]
covid_daily_confirmed <- rbind(covid_daily_confirmed, gbr_national)

# special process for ITA subnats:
# ITA_99999 is made from ITA_35498 and ITA_35499 in allcause data
ita_combine <- copy(covid_daily_confirmed)
ita_combine <- ita_combine[location_id %in% c(35498, 35499)]
ita_combine[, location_id := 99999]
covid_daily_confirmed <- rbind(covid_daily_confirmed, ita_combine)

# aggregate
covid_daily_confirmed <- covid_daily_confirmed[,
  list(daily_cases = sum(daily_cases)),
  by = c("location_id", "date")
]
covid_daily_confirmed <- covid_daily_confirmed[, .(location_id, date, daily_cases)]

# checks
covid_daily_confirmed <- validate_download_data(
  dt = covid_daily_confirmed,
  process_locations = process_locations,
  not_na =   c("location_id", "date", "daily_cases")
)
assertable::assert_values(
  covid_daily_confirmed, "daily_cases", test = "gte", test_val = 0, quiet = TRUE
)

# Final output
readr::write_csv(covid_daily_confirmed, paste0(main_dir,"FILEPATH"))
rm(covid_daily_confirmed)


# SEIR Covariates --------------------------------------------------------------

message(Sys.time(), " | SEIR Covariates")

# read in best version from covid team
testcov <- fread(testing_cov_file)
mobcov <- fread(mobility_cov_file)

testcov <- fill_missing_dates(testcov)
mobcov <- fill_missing_dates(mobcov)

# fill missing values with previous - missing values at beginning of time series
testcov <- testcov[order(date)]
testcov <- testcov %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(mean, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

mobcov <- mobcov[order(date)]
mobcov <- mobcov %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(mean, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

# add ihme loc id
testcov <- merge(
  testcov,
  process_locations[, c("location_id","ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)
mobcov <- merge(
  mobcov,
  process_locations[, c("location_id","ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

# testing checks
testcov <- check_square(testcov)
testcov <- validate_download_data(
  dt = testcov[!is.na(mean)],
  process_locations = process_locations,
  not_na =  c("location_id","date","mean")
)
assertable::assert_values(testcov[!is.na(mean)], "mean", test = "gte", test_val = 0, quiet = T)

# mobility checks
mobcov <- check_square(mobcov)
mobcov <- validate_download_data(
  dt = mobcov[!is.na(mean)],
  process_locations = process_locations,
  not_na =  c("location_id","date","mean")
)

# asked to save our own version to work from
readr::write_csv(testcov, paste0(main_dir, "FILEPATH"))
readr::write_csv(mobcov, paste0(main_dir, "FILEPATH"))
rm(testcov)
rm(mobcov)


# VR Completeness --------------------------------------------------------------

message(Sys.time(), " | VR Completeness")

# fetch ddm outputs from database
run_id_ddm_estimate <- demInternal::get_versions(run_status = "best",
                                                 gbd_year = ddm_gbd_year,
                                                 process_names="ddm estimate")$run_id
completeness <- demInternal::get_dem_outputs(
  process_name = "ddm estimate",
  run_id = run_id_ddm_estimate,
  gbd_year = 2019,
  year_ids = estimation_year_start:estimation_year_end,
  estimate_stage_id = c(11, 14), # u5 completeness & final completeness
  name_cols = T,
  odbc_dir = odbc_dir,
  odbc_section = odbc_section
)
completeness <- completeness[source_type %in% c("VR","DSP")]

# reshape wide on age group (as indicated by 'estimate_stage')
completeness <- dcast(
  data = completeness,
  formula = year_start + location_id + sex ~ estimate_stage,
  value.var = "mean"
)
setnames(
  completeness, c("final completeness", "under-5 comp"), c("adult", "child")
)

# fill missing m/f with both-sex combined for under-5
completeness[,
             child_both_sex := child[sex == "all"],
             by = c("year_start", "location_id")
             ]
completeness[is.na(child), child := child_both_sex]
completeness[, child_both_sex := NULL]

# fill missing subnat locations with national locations
# england and wales
eng_and_wales <- completeness[location_id==95]
eng_and_wales[, location_id := 432]
completeness <- rbind(completeness, eng_and_wales)
# ita 99999
ita99999 <- completeness[location_id %in% c(35498, 35499)] # England & Wales
ita99999[, location_id := 99999]
# make testing reference the mean
ita99999 <- ita99999[,
                   list(adult = mean(adult), child = mean(child)),
                   by = c("year_start","sex")
                   ]
ita99999[, location_id := 99999]
completeness <- rbind(completeness, ita99999)

# fill years 2020 and 2021 with values for 2019
for (yy in 2020:2021){
  temp <- completeness[year_start == 2019]
  temp[,year_start := yy]
  completeness <- completeness[!year_start == yy]
  assertthat::assert_that(!yy %in% unique(completeness$year_start))
  completeness <- rbind(completeness, temp)
}

# make sure completeness is capped at 1
completeness[adult >= 0.95, adult := 1]
completeness[child >= 0.95, child := 1]

# check
completeness <- validate_download_data(
  dt = completeness,
  process_locations = process_locations,
  not_na = names(completeness)
)
assertable::assert_values(completeness, c("adult", "child"), test = "gte", test_val = 0, quiet = T)
assertable::assert_values(completeness, c("adult", "child"), test = "lte", test_val = 1, quiet = T)

readr::write_csv(completeness, fs::path(main_dir, "FILEPATH"))
rm(completeness)


# Daily covid ------------------------------------------------------------------

daily_covid <- fread(daily_cases_file)

# square datasets - Fill dates between min and max for each location
daily_covid <- fill_missing_dates(daily_covid)

# fill missing values with previous - missing values at beginning of time series
daily_covid <- daily_covid[order(date)]
daily_covid <- daily_covid %>%
  dplyr::group_by(location_id) %>%
  tidyr::fill(mean, .direction = "down") %>%
  dplyr::ungroup() %>% setDT

# checks
daily_covid <- check_square(daily_covid)
daily_covid <- validate_download_data(
  dt = daily_covid[!is.na(mean)],
  process_locations = process_locations,
  not_na =  c("location_id","date","mean")
)
assertable::assert_values(daily_covid[!is.na(mean)], "mean", test = "gte", test_val = 0, quiet = T)

# save and remove
readr::write_csv(daily_covid, paste0(main_dir, "FILEPATH"))
rm(daily_covid)


# GBD Inpatient Admissions -----------------------------------------------------

GBD_inpatient_admis <- get_covariate_estimates(covariate_id = 2331,
                                               year_id = 2000:2020,
                                               decomp_step = "iterative",
                                               gbd_round_id = get_gbd_round(gbd_year))

# checks
GBD_inpatient_admis <- validate_download_data(
  dt = GBD_inpatient_admis,
  process_locations = process_locations,
  not_na =  c("location_id","age_group_id","sex_id","mean_value")
)
assertable::assert_values(GBD_inpatient_admis,  c("mean_value"), test = "gte", test_val = 0, quiet = T)

# save and remove
readr::write_csv(GBD_inpatient_admis, paste0(main_dir, "FILEPATH"))
rm(GBD_inpatient_admis)


# Star rating ------------------------------------------------------------------

stars <- fread(star_file)

# checks
stars <- validate_download_data(
  dt = stars,
  process_locations = process_locations,
  not_na =  c("location_id","stars")
)
assertable::assert_values(stars,  c("stars"), test = "gte", test_val = 0, quiet = T)

# save and remove
readr::write_csv(stars, paste0(main_dir, "FILEPATH"))
rm(stars)


# Meta --------------------------------------------------------------------

# Title: Compile stage-1 out-of-sample results and compute ensemble weights


# Settings ----------------------------------------------------------------

## SELECT: directories
code_dir <- "FILEPATH"
base_dir <- "FILEPATH"

## SELECT: run IDs
run_id_oos <- c("RUN_ID")


# Libraries ---------------------------------------------------------------

library(data.table)
library(ggplot2)
library(magrittr)

source(paste0(code_dir, "/functions/R/load_summaries_and_data.R"))


# Out-of-sample Inputs ----------------------------------------------------

dt_oos <- load_summaries_and_data(
  run_ids = run_id_oos,
  base_dir = base_dir
)

dt_oos <- dt_oos[age_name == "0 to 125" & sex == "all" & model_type != "ensemble"]

assertthat::assert_that(
  setequal(
    unique(dt_oos$model_type),
    c("regmod_6", "regmod_12", "regmod_18", "regmod_24", "poisson", "previous_year")
  ),
  msg = "Expected model types 6, 12, 18, and 24 month tail are not all present."
)


# RMSE --------------------------------------------------------------------

# out-of-sample, only keep 2019 post time cutoff
dt_oos_rmse <- dt_oos[
  year_start == 2019 & time_start >= time_cutoff &
  !is.na(death_rate_expected) & !is.na(death_rate_observed)
]

# compute rmse
dt_oos_rmse[, sq_error := (death_rate_expected - death_rate_observed) ^2]

# get one rmse for all location-time
rmse_oos <- dt_oos_rmse[, list(rmse = sqrt(mean(sq_error))), by = "model_type"]

# repeat with separate rmse values by location
rmse_oos_2 <- dt_oos_rmse[,
  list(rmse = sqrt(mean(sq_error))),
  by = c("model_type", "location_id")
]


# Ensemble ----------------------------------------------------------------

# compute weights
weights <- copy(rmse_oos)
weights[, weight := (1/(rmse^2))]
weights[, weight := weight / (sum(weight))]

# save weights
readr::write_csv(
  weights,
  fs::path(code_dir, "/constant_inputs/stage1_ensemble_weights.csv")
)

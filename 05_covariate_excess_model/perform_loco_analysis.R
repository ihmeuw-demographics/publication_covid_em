
# Load libraries ----------------------------------------------------------

library(broom)
library(data.table)
library(purrr)


# Set parameters ----------------------------------------------------------

current_date <- Sys.Date()
path_input_data <- fs::path("FILEPATH")

dir_code <- fs::path("FILEPATH")
path_model_specifications <- fs::path("specify_models.R")


# Load maps ---------------------------------------------------------------

map_locs_covid <- demInternal::get_locations(gbd_year = 2020, location_set_name = "COVID-19 modeling")

map_locs <- copy(map_locs_covid)


# Load data ---------------------------------------------------------------

dt_input <- fread(path_input_data)


# Specify model -----------------------------------------------------------

source(path_model_specifications)

model_list <- specify_models(c(
  "m1_cdr.death_rate_excess"
))


# Define functions --------------------------------------------------------

calc_region_residuals <- function(dt_resid, loc_map) {

  map_region_draws <- loc_map[level == 2, .(super_region_name, region_name)]

  dt_region_resid <- dt_resid[
    ,
    .(resid_region = mean(residual, na.rm = TRUE)),
    by = .(region_name)
  ]

  dt_region_resid <- dt_region_resid[
    map_region_draws,
    on = .(region_name)
  ]

  dt_super_region_resid <- dt_resid[
    ,
    .(resid_super_region = mean(residual, na.rm = TRUE)),
    by = .(super_region_name)
  ]

  dt_region_resid[
    dt_super_region_resid,
    resid_super_region := i.resid_super_region,
    on = .(super_region_name)
  ]

  dt_region_resid[, .(
    super_region_name,
    region_name,
    residual = fifelse(is.na(resid_region), resid_super_region, resid_region)
  )]

}

loo_func <- function(loc, model_formula, dt) {

  dt_sub <- dt[location_id != loc]

  m <- lm(model_formula, data = dt_sub)

  dt_m_aug <- setDT(augment(m, newdata = dt_sub))
  setnames(dt_m_aug, ".resid", "residual")
  dt_resid <- calc_region_residuals(dt_m_aug, map_locs)
  dt_resid[, residual := fifelse(is.na(residual), mean(residual, na.rm = TRUE), residual)]

  pred <- predict(m, newdata = dt[location_id == loc])

  dt_pred <- dt_resid[
    region_name == map_locs[location_id == loc, region_name],
    .(
      location_id = loc,
      data_val = dt[location_id == loc, death_rate_excess],
      pred_val = exp(pred + residual)
    )
  ]

  return(dt_pred)

}


# Perform loo validation --------------------------------------------------

run_loo <- function(model_formula, dt) {
  setDT(purrr::map_dfr(
    unique(dt$location_id),
    ~loo_func(loc = .x, model_formula = model_formula, dt = dt)
  ))
}

dt_loo_results <- setDT(purrr::map_dfr(model_list, run_loo, dt = dt_input, .id = "model"))

dt_loo_results[, relative_error := (pred_val - data_val) / data_val]


# Compile summary stats ---------------------------------------------------

dt_summary <- dt_loo_results[
  !is.na(data_val),
  .(
    rsme = Metrics::rmse(data_val, pred_val),
    mrae = mean(abs((pred_val - data_val) / data_val)),
    mean_relative_error = mean(relative_error)
  ),
  by = .(model)
]

dt_summary[order(model)]


# Save results ------------------------------------------------------------

readr::write_csv(
  dt_summary[order(model)],
  fs::path("FILEPATH")
)

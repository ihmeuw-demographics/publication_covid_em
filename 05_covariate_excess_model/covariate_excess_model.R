
# Load libraries ----------------------------------------------------------

library(broom)
library(data.table)
library(purrr)


# Set parameters ----------------------------------------------------------

current_date <- Sys.Date()
timestamp_id <- format(Sys.time(), "%Y-%m-%d-%H-%M")
run_id <- paste0(timestamp_id)

set.seed(4567)
n_draws <- 100

base_dir <- fs::path("FILEPATH")
out_dir <- fs::path(base_dir, run_id, "outputs")

dir_code <- fs::path("FILEPATH")
path_model_specifications <- fs::path("specify_models.R")

path_dt_list <- list(
  fit = "FILEPATH",
  pred_all_draw = "FILEPATH"
)


# Load maps ---------------------------------------------------------------

map_locs_covid <- demInternal::get_locations(gbd_year = 2020, location_set_name = "COVID-19 modeling")

map_locs <- copy(map_locs_covid)


# Load data ---------------------------------------------------------------

dt_list_raw <- purrr::map(path_dt_list, fread)


# Prep data ---------------------------------------------------------------

# Prep locations to exclude from prediction
locs_nonestimate <- map_locs[
  ihme_loc_id %in% c("TJK", "TKM", "TZA", "NIC", "PRK", "VEN"),
  location_id
]

# Fit and Prediction covariate prep

prep_cov_general <- function(data) {

  data_prep <- data[location_id %in% map_locs$location_id]

  data_prep[
    map_locs,
    `:=`(
      ihme_loc_id = i.ihme_loc_id,
      region_name = i.region_name,
      super_region_name = i.super_region_name
    ),
    on = .(location_id)
  ]

  data_prep[is.na(stars), stars := -1]
  data_prep[stars <= 3, stars_bin := "low"]
  data_prep[stars >= 4, stars_bin := "high"]
  data_prep[, stars := as.factor(stars)]
  data_prep[, stars_bin := as.factor(stars_bin)]

  data_prep[, prop_ncd := ncd_death_rate / crude_death_rate]

  data_prep[, ci_person_years := NULL]

  return(data_prep)

}

prep_cov_specific <- function(data, type) {

  stopifnot(type %like% "fit|pred")

  data <- copy(data)

  if (isTRUE(type %like% "fit")) {

    # Fit data specific prep

    data[, ratio := death_rate_excess / death_rate_covid]

    data <- data[!is.na(death_rate_excess) & death_rate_excess >= 0]
    data[, outlier := FALSE]
    data[death_rate_covid == 0, outlier := TRUE]

    data[ihme_loc_id %in% c("CHN_354", "GTM"), outlier := TRUE]
    data[ihme_loc_id %like% "RUS_", outlier := TRUE]

    data <- data[!(outlier), -c("idr_reference", "mobility_reference", "outlier")]

    assertable::assert_values(data, colnames(data), test = "not_na")

    assertable::assert_values(
      data,
      grep("rate$", colnames(data), value = TRUE),
      test = "gte",
      test_val = 0
    )

  } else if (isTRUE(type %like% "pred")) {

    # Prediction data specific prep

    data <- data[!location_id %in% locs_nonestimate]
    data <- data[!location_id %in% c(95, 99999)]

    data <- data[
      !is.na(cumulative_infections) & !is.na(universal_health_coverage),
      -c("idr_reference", "mobility_reference", "stars")
    ]

    data <- data[death_rate_covid != 0]

    assertable::assert_values(
      data,
      setdiff(colnames(data), c("death_rate_excess", "death_rate_person_years", "ratio")),
      test = "not_na"
    )

    assertable::assert_values(
      data,
      grep("rate$", colnames(data), value = TRUE),
      test = "gte",
      test_val = 0
    )

  }

  return(data)

}

dt_list_raw$fit <- dt_list_raw$fit[location_id != 128]

dt_list_prep <- dt_list_raw %>%
  purrr::map(prep_cov_general) %>%
  purrr::imap(~prep_cov_specific(data = .x, type = .y))

# Add prediction set with 0 mobility and max IDR lagged
make_reference_frames <- function(dt_list, frame_names) {

  dt_list_ref <- dt_list[frame_names] %>%
    purrr::map(function(dt) {
      dt <- copy(dt)
      dt[, `:=`(
        idr_lagged = max(idr_lagged)
      )]
    })

  frame_ref_names <- paste0(frame_names, ".reference")
  names(dt_list_ref) <- frame_ref_names

  return(dt_list_ref)

}

dt_list_prep_ref <- make_reference_frames(
  dt_list_prep,
  grep("pred", names(dt_list_prep), value = TRUE)
)
dt_list_prep <- c(dt_list_prep, dt_list_prep_ref)
rm(dt_list_prep_ref)


# Specify models ----------------------------------------------------------

source(path_model_specifications)

formula_list <- specify_models(c(
  "m1_cdr.death_rate_excess"
))


# Fit models --------------------------------------------------------------

dt_list_fit <- split(dt_list_prep$fit, by = "draw")

dt_model_params <- CJ(
  formula = formula_list,
  data = dt_list_fit,
  sorted = FALSE
)

dt_model_param_names <- CJ(
  formula_name = names(formula_list),
  draw = as.integer(names(dt_list_fit)),
  sorted = FALSE
)

dt_model_params[, rn := .I]
dt_model_param_names[, rn := .I]

dt_model <- merge(dt_model_param_names, dt_model_params, by = "rn")
dt_model[, rn := NULL]
rm(dt_model_params, dt_model_param_names)

dt_model[, model := purrr::pmap(.(formula, data), lm)]

dt_model[, c("model_name", "response") := tstrsplit(formula_name, ".", fixed = TRUE)]
dt_model[, c("formula_name") := NULL]
setcolorder(dt_model, c("model_name", "response"))


# Extract model summaries -------------------------------------------------

dt_model_summary <- dt_model[, .(
  model = model_name,
  response,
  draw,
  dt_tidy = purrr::map(model, tidy),
  dt_glance = purrr::map(model, glance)
)]

dt_coef <- setDT(tidyr::unnest(dt_model_summary[, -"dt_glance"], dt_tidy))
dt_rsq <- setDT(tidyr::unnest(dt_model_summary[, -"dt_tidy"], dt_glance))
dt_rsq <- dt_rsq[, .(model, response, draw, rsq = r.squared)]

dt_coef_wide <- dcast(
  dt_coef,
  ...~model,
  value.var = c("estimate", "std.error", "statistic", "p.value")
)


# Get model coefficients --------------------------------------------------

dt_model_coef <- dt_model[
  ,
  .(
    formula = unique(formula),
    coefs = .(purrr::map_dfr(model, ~as.data.table(t(coef(.x)))))
  ),
  by = .(model_name, response)
]

dt_model_coef[, coefs := purrr::map(coefs, as.matrix)]


# Get model residuals -----------------------------------------------------

calc_region_residuals <- function(dt_resid, loc_map) {

  map_region_draws <- loc_map[level == 2, .(super_region_name, region_name)]
  map_region_draws <- rbindlist(replicate(n_draws, map_region_draws, simplify = FALSE), idcol = "draw")

  dt_region_resid <- dt_resid[
    ,
    .(resid_region = mean(residual, na.rm = TRUE)),
    by = .(draw, region_name)
  ]

  dt_region_resid <- dt_region_resid[
    map_region_draws,
    on = .(region_name, draw)
  ]

  dt_super_region_resid <- dt_resid[
    ,
    .(resid_super_region = mean(residual, na.rm = TRUE)),
    by = .(draw, super_region_name)
  ]

  dt_region_resid[
    dt_super_region_resid,
    resid_super_region := i.resid_super_region,
    on = c("draw", "super_region_name")
  ]

  dt_region_resid[, .(
    draw,
    super_region_name,
    region_name,
    residual = fifelse(is.na(resid_region), resid_super_region, resid_region)
  )]

}

keep_cols_resid <- c(
  "location_id", "ihme_loc_id", "super_region_name", "region_name", ".resid"
)

dt_residuals <- dt_model[, .(
  model_name, response, draw,
  dt_resid = purrr::pmap(.(x = model, newdata = data), augment)
)]

dt_residuals[, dt_resid := purrr::map(dt_resid, ~setDT(.x[, ..keep_cols_resid]))]
dt_residuals <- setDT(tidyr::unnest(dt_residuals, dt_resid))
setnames(dt_residuals, ".resid", "residual")

dt_residuals <- dt_residuals[
  ,
  .(
    dt_resid = list(.SD),
    dt_resid_region = list(calc_region_residuals(.SD, map_locs))
  ),
  by = .(model_name, response)
]


# Predict from model ------------------------------------------------------

predict_draws <- function(coefs,
                          formula,
                          dt_pred_frame,
                          dt_resid_insample,
                          dt_resid_region,
                          add_insample_residual) {

  # Fixed effect predictions

  cov_formula <- update.formula(formula, NULL ~ .)

  dt_pred <- split(dt_pred_frame, by = c("location_id", "region_name")) %>%
    purrr::map_dfr(
      ~rowSums(model.matrix(cov_formula, data = .x) * coefs),
      .id = "loc_region"
    ) %>%
    setDT() %>%
    melt(
      id.vars = "loc_region",
      variable.name = "draw",
      value.name = "baseline",
      variable.factor = FALSE
    )

  dt_pred[, c("location_id", "region_name") := tstrsplit(
    loc_region, ".", type.convert = TRUE, fixed = TRUE
  )]

  dt_pred[, `:=`(
    loc_region = NULL,
    draw = as.integer(draw)
  )]

  # Append region-level residuals

  dt_pred[
    dt_resid_region,
    resid_region := i.residual,
    on = .(draw, region_name)
  ]

  # Append in-sample residual

  # Use mean for India in-sample residual
  dt_resid_insample[
    ihme_loc_id %like% "IND",
    residual := mean(residual),
    by = .(draw)
  ]

  dt_pred[
    dt_resid_insample,
    resid_insample := i.residual,
    on = .(draw, location_id)
  ]

  # Align predictions with input data

  if (add_insample_residual) {

    dt_pred[, with_resid := baseline + fifelse(is.na(resid_insample), resid_region, resid_insample)]

  } else {

    dt_pred[, with_resid := baseline + resid_region]

  }

  dt_pred[, .(location_id, draw, baseline, with_resid)]

}

# Create prediction argument lists

## NOTE:
#  Make sure the parameter index and names tables are specified in the same
#  order

dt_pred_params <- copy(dt_model_coef)
dt_pred_params[
  dt_residuals,
  `:=`(
    dt_resid_insample = i.dt_resid,
    dt_resid_region = i.dt_resid_region
  ),
  on = .(model_name, response)
]

dt_data_frame <- CJ(
  model_name = unique(dt_pred_params$model_name),
  data_frame = names(dt_list_prep),
  sorted = FALSE
)

dt_data_frame[
  setDT(tibble::enframe(dt_list_prep, name = "data_frame", value = "dt_pred_frame")),
  dt_pred_frame := i.dt_pred_frame,
  on = "data_frame"
]

dt_data_frame <- dt_data_frame[data_frame != "fit"]

dt_pred_params <- dt_pred_params[dt_data_frame, on = "model_name"]

dt_pred_params[, dt_preds := purrr::pmap(
  .(coefs, formula, dt_pred_frame, dt_resid_insample, dt_resid_region),
  predict_draws,
  add_insample_residual = TRUE
)]

dt_pred <- setDT(tidyr::unnest(
  dt_pred_params[, .(model = model_name, response, data_frame, dt_preds)],
  dt_preds
))

dt_pred <- melt(
  dt_pred,
  id.vars = c("model", "response", "data_frame", "location_id", "draw"),
  variable.name = "pred_type"
)

dt_pred[, value := exp(value)]


# Separate reference frames -----------------------------------------------

dt_pred[, c("data_frame", "scenario") := tstrsplit(data_frame, ".", fixed = TRUE)]
dt_pred[is.na(scenario), scenario := "prediction"]


# Substitute negative excess deaths ---------------------------------------

dt_pred[
  dt_list_raw$fit,
  death_rate_excess_input := i.death_rate_excess,
  on = .(location_id, draw)
]

dt_pred[
  scenario == "prediction" & pred_type == "with_resid" & death_rate_excess_input < 0,
  value := death_rate_excess_input
]

dt_pred[, death_rate_excess_input := NULL]


# Format prediction results for aggregation/scaling -----------------------

dt_covid_person_years <- rbindlist(
  dt_list_raw,
  idcol = "data_frame",
  fill = TRUE,
  use.names = TRUE
)

dt_covid_person_years <- dt_covid_person_years[, .(
  data_frame,
  draw,
  location_id,
  person_years,
  person_years_covid = covid_person_years,
  death_rate_covid
)]

dt_pred <- dcast(dt_pred, ...~response, value.var = "value")

dt_pred[
  dt_covid_person_years,
  `:=`(
    person_years = i.person_years,
    person_years_covid = i.person_years_covid,
    deaths_covid = i.person_years_covid * i.death_rate_covid,
    deaths_excess = i.person_years * death_rate_excess
  ),
  on = .(data_frame, draw, location_id)
]

dt_pred[, "death_rate_excess" := NULL]


# Add reference ratios ----------------------------------------------------

dt_pred_ratios <- dcast(
  dt_pred,
  ... ~ scenario + pred_type,
  value.var = "deaths_excess",
  sep = "."
)

dt_pred_ratios <- dt_pred_ratios[, .(
  model, data_frame, location_id, draw,
  person_years, person_years_covid,
  deaths_covid,
  deaths_excess = prediction.with_resid,
  true_covid_prop_excess = reference.baseline / prediction.baseline,
  ratio_covid_excess = deaths_covid / prediction.with_resid
)]

dt_pred_ratios[
  deaths_covid == 0 & deaths_excess <= 0,
  c("true_covid_prop_excess", "ratio_covid_excess") := 1
]

dt_pred_ratios[, `:=`(
  ratio_true_reported_covid = pmax(true_covid_prop_excess, ratio_covid_excess) / ratio_covid_excess
)]

dt_pred_ratios[ratio_covid_excess > 1, ratio_true_reported_covid := 1]

dt_pred_ratios[
  deaths_covid == 0 & deaths_excess == 0,
  ratio_true_reported_covid := 1
]

dt_pred_ratios[deaths_excess < 0, ratio_true_reported_covid := 1]

# Convert to count for scaling aggregation
dt_pred_ratios[, `:=`(
  deaths_excess_reference = deaths_excess * true_covid_prop_excess,
  ratio_true_reported_covid_count = deaths_covid * ratio_true_reported_covid,
  true_covid_prop_excess = NULL,
  ratio_covid_excess = NULL,
  ratio_true_reported_covid = NULL
)]


# Scale -------------------------------------------------------------------

scale_agg_id_cols <- c("model", "data_frame", "location_id", "draw")
scale_agg_value_cols <- setdiff(colnames(dt_pred_ratios), scale_agg_id_cols)

scale_func <- function(dt_pred, dt_map) {

  dt_map_sub <- dt_map[child %in% unique(dt_pred$location_id)]

  hierarchyUtils::scale(
    dt = dt_pred[location_id %in% unique(c(dt_map_sub$child, dt_map_sub$parent))],
    id_cols = scale_agg_id_cols,
    value_cols = scale_agg_value_cols,
    col_stem = "location_id",
    col_type = "categorical",
    mapping = dt_map_sub,
    agg_function = sum
  )

}

scale_loc_map <- map_locs[
  ihme_loc_id %like% "CAN|DEU|ESP" & level > 3,
  .(child = location_id, parent = parent_id, nat_ihme_loc = substr(ihme_loc_id, 1, 3))
]

dt_pred_scaled <- purrr::map_dfr(
  split(scale_loc_map, by = "nat_ihme_loc", keep.by = FALSE),
  ~scale_func(dt_pred_ratios, dt_map = .x)
)

dt_pred_pre_agg <- rbind(
  dt_pred_ratios[!location_id %in% scale_loc_map$child],
  dt_pred_scaled[location_id %in% scale_loc_map$child],
  use.names = TRUE
)


# Aggregate ---------------------------------------------------------------

agg_func <- function(dt, mapping) {

  dt_missing_locs <- dt[
    ,
    .(location_id = setdiff(mapping[level >= 3, child], location_id)),
    by = setdiff(scale_agg_id_cols, "location_id")
  ]

  dt_missing_locs[, (scale_agg_value_cols) := 0]

  dt_agg <- hierarchyUtils::agg(
    rbind(dt, dt_missing_locs),
    id_cols = scale_agg_id_cols,
    value_cols = scale_agg_value_cols,
    col_stem  = "location_id",
    col_type = "categorical",
    mapping = map_loc_agg[, -"level"],
    agg_function = sum,
    present_agg_severity = "none",
    missing_dt_severity = "warning",
    na_value_severity = "warning"
  )

  dt_agg_combined <- rbind(
    dt[!location_id %in% unique(dt_agg$location_id)],
    dt_agg
  )

  return(dt_agg_combined)

}

# Create mapping
map_loc_agg <- map_locs[location_id != 1, .(child = location_id, parent = parent_id, level)]

dt_agg <- agg_func(dt_pred_pre_agg, map_loc_agg)


# Fill missing location with parent values --------------------------------

dt_fill_locs <- dt_covid_person_years[location_id %in% locs_nonestimate]

dt_fill_locs <- dt_fill_locs[
  unique(dt_agg[, .(model, data_frame)]),
  on = "data_frame",
  allow.cartesian = TRUE
]

dt_fill_locs[map_locs, parent_id := i.parent_id, on = "location_id"]

# Use China to set PRK COVID death rate
dt_fill_locs[location_id == 7, parent_id := 6]

dt_fill_locs[
  dt_agg,
  parent_death_rate_covid := i.deaths_covid / i.person_years,
  on = .(parent_id = location_id, draw)
]

dt_fill_locs[is.na(death_rate_covid), death_rate_covid := parent_death_rate_covid]
dt_fill_locs[is.na(person_years_covid), person_years_covid := 0]
dt_fill_locs[, `:=`(
  deaths_covid = death_rate_covid * person_years,
  death_rate_covid = NULL,
  parent_death_rate_covid = NULL
)]

dt_fill_locs[
  dt_agg,
  `:=`(
    deaths_excess = i.deaths_excess / i.person_years * person_years,
    deaths_excess_reference = i.deaths_excess_reference / i.person_years * person_years,
    ratio_true_reported_covid_count = i.ratio_true_reported_covid_count / i.deaths_covid * deaths_covid
  ),
  on = .(model, data_frame, draw, parent_id = location_id)
]

dt_fill_locs[, parent_id := NULL]


# Re-aggregate with missing locations filled ------------------------------

dt_pred_pre_agg2 <- rbind(dt_pred_pre_agg, dt_fill_locs, use.names = TRUE)
dt_agg_filled <- agg_func(dt_pred_pre_agg2, map_loc_agg)


# Recalculate ratios ------------------------------------------------------

dt_agg_filled[, `:=`(
  true_covid_prop_excess = deaths_excess_reference / deaths_excess,
  ratio_covid_excess = deaths_covid / deaths_excess,
  ratio_true_reported_covid = ratio_true_reported_covid_count / deaths_covid,
  deaths_excess_reference = NULL,
  ratio_true_reported_covid_count = NULL
)]

dt_agg_filled[
  deaths_covid == 0 & deaths_excess == 0,
  c("true_covid_prop_excess", "ratio_covid_excess", "ratio_true_reported_covid") := 1
]

dt_agg_filled[
  deaths_covid == 0 & deaths_excess != 0,
  ratio_true_reported_covid := 1
]

# Set minimum draw-level ratio to 1, which can end up below 1 after scaling
dt_agg_filled[ratio_true_reported_covid < 1, ratio_true_reported_covid := 1]


# Add excess / COVID ratio ------------------------------------------------

dt_agg_filled[, ratio_excess_covid := deaths_excess / deaths_covid]

dt_agg_filled[
  deaths_covid == 0 & deaths_excess == 0,
  ratio_excess_covid := 1
]


# Expert prior adjustments ------------------------------------------------

loc_ids_ratio1 <- map_locs[ihme_loc_id %like% "RUS", location_id]

locs_s1_neg_excess <- dt_list_raw$fit[
  ,
  .(death_rate_excess = mean(death_rate_excess)),
  by = .(location_id)
][death_rate_excess < 0, location_id]

dt_agg_filled[
  location_id %in% c(loc_ids_ratio1, locs_s1_neg_excess),
  ratio_true_reported_covid := 1
]


# Shuffle draws -----------------------------------------------------------

new_draw_order <- sample(n_draws)
dt_agg_filled[
  order(draw),
  draw := new_draw_order,
  by = .(location_id, model, data_frame)
]


# Summarize results -------------------------------------------------------

summary_id_cols <- c(scale_agg_id_cols, "person_years", "person_years_covid", "deaths_covid")
summary_val_cols <- setdiff(colnames(dt_agg_filled), summary_id_cols)

dt_pred_agg_summary <- demUtils::summarize_dt(
  dt_agg_filled,
  id_cols = summary_id_cols,
  summarize_cols = "draw",
  value_cols = summary_val_cols
)

quantile_colnames <- grep("q2\\.5|q97\\.5", colnames(dt_pred_agg_summary), value = TRUE)

if (uniqueN(dt_agg_filled$draw) == 1) {

  dt_pred_agg_summary <- dt_pred_agg_summary[, -..quantile_colnames]

} else {

  dt_pred_agg_summary[!data_frame %like% "draw", (quantile_colnames) := NA]

}


# Format results ----------------------------------------------------------

# Add location metadata

dt_pred_final <- map_locs[, c(1:3, 8, 10)][
  dt_pred_agg_summary,
  on = "location_id"
]

dt_pred_draw_final <- map_locs[, c(1:3, 8, 10)][
  dt_agg_filled,
  on = "location_id"
]


# Save results ------------------------------------------------------------

readr::write_csv(
  dt_coef_wide,
  fs::path(out_dir, paste0("model_coefs-", run_id, ".csv"))
)

readr::write_csv(
  dt_rsq,
  fs::path(out_dir, paste0("model_rsq-", run_id, ".csv"))
)

readr::write_csv(
  dt_pred_final,
  fs::path(out_dir, paste0("model_prediction-ratios-summary-", run_id, ".csv"))
)

readr::write_csv(
  dt_pred_draw_final,
  fs::path(out_dir, paste0("model_prediction-ratios-draws-", run_id, ".csv"))
)

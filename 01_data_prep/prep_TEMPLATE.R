
# Meta --------------------------------------------------------------------

# Description: Prepare TEMPLATE all-cause data for COVID excess mortality model
# Steps:
#   1. Loads config file
#   2. Loads input data from `raw_data` directory
#   3. Prepares to our format (column names etc.) as described here:
#   4. Saves formatted data to `formatted` directory


# Setup -------------------------------------------------------------------

library(data.table)
library(ggplot2)
library(fs)


raw_dir <- "FILEPATH"
formatted_dir <- "FILEPATH"

# Date used to identify the version of the raw data file
download_date <- "DATE"

ihme_loc_id <- "TEMPLATE"
source <- "TEMPLATE"


# Load --------------------------------------------------------------------

dt <- fread(fs::path("FILEPATH"))


# Format ------------------------------------------------------------------


# TEMPLATE: add code to format columns according to Hub documentation

# add meta
dt[, ":=" (nid = NA, underlying_nid = NA, source = source, source_type = NA,
           location_note = NA, week_start = NA, ihme_loc_id = NA,
           week_end = NA, month_end = month_start, day_start = NA, day_end = NA,
           sex_id = 3, age_start = 0, age_end = 125, date_reported = NA,
           cause_id = 294)]

# subset to needed cols
keep <- c("nid", "underlying_nid", "source", "source_type", "ihme_loc_id",
          "location_note", "year_start", "week_start", "week_end", "month_start",
          "month_end", "day_start", "day_end", "sex_id", "age_start", "age_end",
          "date_reported", "cause_id", "deaths")
dt_final <- dt[, ..keep]


# Check ------------------------------------------------------------------------

assertable::assert_values(dt_final, "deaths", test = "gte", test_val = 0, quiet = T)
assertable::assert_values(dt_final, c('ihme_loc_id','year_start','month_start',
                                      'sex_id','age_start', 'deaths'),
                          test = "not_na", quiet = T)


# Save --------------------------------------------------------------------

locs <- sort(unique(dt_final$ihme_loc_id))
date <- gsub('-', '_', Sys.Date())

for (loc in locs){
  # create dir
  outdir <- fs::path(formatted_dir, loc, source, date)
  dir.create(outdir, recursive = T)

  # save deaths
  for (yy in unique(dt_final[ihme_loc_id==loc]$year_start)) {
    print(paste0(loc, " ", yy))
    readr::write_csv(
      dt_final[year_start == yy & ihme_loc_id == loc],
      fs::path(outdir, paste0(loc, "_", source, "_", yy, ".csv"))
    )
  }
}


# Plot -------------------------------------------------------------------------

# Change to weeks and aggregate if applicable

diagnostics_path <- file.path("FILEPATH")
lapply(file.path(diagnostics_path), dir.create, recursive = T)

hierarchy <- demInternal::get_locations(gbd_year = 2020, location_set_name = "Model Results")

dt_final <- merge(
  dt_final,
  hierarchy[,c("ihme_loc_id","location_name")],
  by = "ihme_loc_id",
  all.x = T
)

pdf(paste0(diagnostics_path, "/raw_deaths_over_months.pdf"), height = 10, width = 16)

locs <- sort(unique(dt_final$ihme_loc_id))

for (loc in locs) {
  for (ss in unique(dt_final$sex_id)){
    plot_data <- dt_final[ihme_loc_id == loc & sex_id == ss]
    ## all ages
    plot_data <- plot_data[, .(deaths=sum(as.numeric(deaths))), by=c('ihme_loc_id','source','location_name','year_start','month_start')]
    plot_data$month_start <- as.numeric(plot_data$month_start)
    plot_data[, time_id := year_start + (month_start - 1) / 12]
    fig <- ggplot() +
      theme_classic() +
      geom_point(data = plot_data, aes(x = time_id, y = deaths, color = source)) +
      geom_line(data = plot_data, aes(x = time_id, y = deaths, color = source)) +
      scale_x_continuous(breaks = unique(plot_data$year_start)) +
      labs(x = 'Month', y = 'Deaths', title = paste0("Deaths by month (all ages, sex: ",ss,"): ",
                                                    unique(plot_data$location_name)))

    print(fig)
  }
}

dev.off()

library(mlbgameday)
library(doParallel)
library(DBI)
library(RSQLite)

# First we need to register our parallel cluster.
no_cores <- detectCores() - 1
cl <- makeCluster(no_cores)  
registerDoParallel(cl)

# Create the database in our working directory.
con <- dbConnect(RSQLite::SQLite(), dbname = "mlb-gameday.db")

# Collect all games, including pre and post-season for the 2016 season.
get_payload(start = "2015-01-01", end = "2019-05-01", db_con = con)

# Don't forget to stop the cluster when finished.
stopImplicitCluster()
rm(cl)

#!/bin/bash

# Stop on any error
set -e

DB_HOST=localhost
DB_PORT=5432
DB_USER=username

# Need to specify import file
if [ -z "$1" ]
then
    echo "No import file specified, specificy feed.zip or feed directory."
    exit 1
else
    FEED_PATH=$1
fi

# Use default database if name not specified in second arguement
if [ -z "$2" ]
then
    DB_NAME=gtfs_tmp
else
    DB_NAME=$2
fi

# Ignore any errors and continue
set +e

psql -h $DB_HOST -p $DB_PORT -U postgres -d postgres -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_USER";
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "CREATE EXTENSION postgis";

cat gtfs_tables.sql \
  <(python2 import_gtfs_to_sql.py $FEED_PATH) \
  gtfs_tables_makespatial.sql \
  gtfs_tables_makeindexes.sql \
  gtfs_calculate_shape_dist_traveled.sql \
  gtfs_calculate_stop_times_dist_traveled.sql \
  vacuumer.sql \
| psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME

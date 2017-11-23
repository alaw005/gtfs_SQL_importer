#!/bin/bash

GTFS_TO_SQL_PATH=../src
GTFS_SOURCE_PATH=~/gtfs_source_folder
DB_NAME=gtfs_database
DB_OWNER=postgres
DB_HOST=localhost
DB_PORT=5432

psql -h $DB_HOST -p $DB_PORT -U postgres -d postgres -c "CREATE DATABASE $DB_NAME WITH OWNER $DB_OWNER";
psql -h $DB_HOST -p $DB_PORT -U postgres -d $DB_NAME -c "CREATE EXTENSION postgis";
psql -h $DB_HOST -p $DB_PORT -U postgres -d $DB_NAME -c "CREATE EXTENSION postgis_topology";

cat $GTFS_TO_SQL_PATH/gtfs_tables.sql \
  <(python2 $GTFS_TO_SQL_PATH/import_gtfs_to_sql.py $GTFS_SOURCE_PATH) \
  $GTFS_TO_SQL_PATH/gtfs_tables_makespatial.sql \
  $GTFS_TO_SQL_PATH/gtfs_tables_makeindexes.sql \
  $GTFS_TO_SQL_PATH/gtfs_calculate_shape_dist_traveled.sql \
  $GTFS_TO_SQL_PATH/gtfs_calculate_stop_times_dist_traveled.sql \
  $GTFS_TO_SQL_PATH/vacuumer.sql \
  $GTFS_TO_SQL_PATH/gtfs_scripts.sql \
| psql -h $DB_HOST -p $DB_PORT -U $DB_OWNER -d $DB_NAME


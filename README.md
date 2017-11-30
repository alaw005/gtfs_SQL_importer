# Updates
- 9 Aug 2017 - Added example script template for import. Also some SQL scripts to generate statistics
- 25 Sep 2015 - Update to include bikes fields in updated GTFS. Tested with Wellington GTF
- 24 Nov 2017 - Revised to make easier to run using bash script
- 29 Nov 2017 - Add calculation of shape distance and stop_time_distance on import
- 1 Dec 2017 - Add interpolation of arrival/departure times and updated distance travelled script

# Quick start

This quick start uses the run_import bash script (Ubuntu):

1. Download source using git.
        git clone https://github.com/alaw005/gtfs_SQL_importer.git

2. Edit the database settings (DB_HOST, DB_PORT, DB_USER) in the bash script to match your local settings:
        nano src/run_import.sh

3. Import feed (NB: You need to run from within the src folder):
        cd src
        bash run_import.sh feed.zip dbname

If dbname already exists, the script will replace all gtfs data (delete and recreated 
tables) and if you do not specify a dbname then "gtfs_tmp" will be used as the database.

Also, feed.zip should be path to zipped version of a feed or to a folder with relevant 
.txt files in level level.
 
4. Done!

All other instructions below are older, most prior to fork. Note latest changes are only 
tested with postgis database and likely will not work in sqlite.



# About
Quick & easy import of GTFS data into a SQL database.

* [GTFS (General Transit Feed Specification)](http://code.google.com/transit/spec/transit_feed_specification.html)
* [List of Public GTFS feeds](http://code.google.com/p/googletransitdatafeed/wiki/PublicFeeds)

# License
Released under the MIT (X11) license. See LICENSE in this directory.


# How To
This is how to import GTFS data into SQL:
( see below for SQLite)

## Initial Import

### PostgreSQL (COPY support)
    cat gtfs_tables.sql \
      <(python import_gtfs_to_sql.py path/to/gtfs/data/directory) \
      gtfs_tables_makeindexes.sql \
      vacuumer.sql \
    | psql mydbname

### PostGIS (spatially enable your tables)
    cat gtfs_tables.sql \
      <(python import_gtfs_to_sql.py path/to/gtfs/data/directory) \
      gtfs_tables_makespatial.sql \
      gtfs_tables_makeindexes.sql \
      vacuumer.sql \
    | psql mydbname

### Other Relational Databases (INSERT support)
This will use "INSERT" statements instead of "COPY" statements.
Also, I believe the vacuumer.sql file is also postgres specific, so omit it if 
it gives errors.

    cat gtfs_tables.sql \
      <(python import_gtfs_to_sql.py path/to/gtfs/data/directory nocopy) \
      gtfs_tables_makeindexes.sql \
      vacuumer.sql \
    | psql mydbname

Most GTFS data has errors in it, so you will likely encounter an error when 
running the step above. After fixing the error by manually correcting the GTFS 
files, you can simply repeat the command (which will likely break again, and 
so on).

## Modification within SQL Database

If you are editing data within the SQL database, it is usually much faster to 
drop all the indexes first and then reapply them afterwards:

    psql -f gtfs_tables_dropindexes.sql
    # do your stuff
    psql -f gtfs_tables_makeindexes.sql


# Test/Demonstration

The corrected (even google's example data has errors) demo feed from the 
GTFS website is included in this distribution. You should play around with that 
first to get everything to work and to see how the data gets put into tables.

From this directory (assuming postgres):

    createdb testgtfs
    cat gtfs_tables.sql \
      <(python import_gtfs_to_sql.py sample_feed) \
      gtfs_tables_makeindexes.sql \
      vacuumer.sql \
    | psql testgtfs
    psql testgtfs -c "\dt"

# Special Cases

## SQLite
Contributed by Justin Jones, Feb 07, 2011  bjustjones@netscape.net

### Initial Import
Note:
  From http://www.sqlite.org/omitted.html as of Feb, 2011:
    "Only the RENAME TABLE and ADD COLUMN variants of the ALTER TABLE command 
    are supported. Other kinds of ALTER TABLE operations such as DROP COLUMN, 
    ALTER COLUMN, ADD CONSTRAINT, and so forth are omitted."

  gtfs_tables.sqlite includes the constraints on creation. 

    cat gtfs_tables.sqlite \
      <(python import_gtfs_to_sql.py sample_feed nocopy)  \
    | sqlite3 ANewDatabase.db

SQLite doesn't enforce constraints by default. See first section of 
gtfs_tables.sqlite for line to change. Move it to the end?
If you need to makeindices or dropindices as above you'll have to experiment
with doing it yourself.

### Modification within SQL Database
    sqlite3 -init gtfs_tables_dropindexes.sqlite myDatabase.db
    # do your stuff
    sqlite3 -init gtfs_tables_makeindexes.sqlite myDatabase.db

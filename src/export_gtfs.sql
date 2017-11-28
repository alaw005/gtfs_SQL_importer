    
DROP FUNCTION IF EXISTS my_export_tables(text[]);
CREATE OR REPLACE FUNCTION my_export_tables(
    gtfs_tables text[] DEFAULT ARRAY['gtfs_agency', 
                                     'gtfs_calendar', 
                                     'gtfs_calendar_dates', 
                                     'gtfs_fare_rules', 
                                     'gtfs_feed_info', 
                                     'gtfs_frequencies', 
                                     'gtfs_routes', 
                                     'gtfs_shapes', 
                                     'gtfs_stops', 
                                     'gtfs_stop_times', 
                                     'gtfs_transfers', 
                                     'gtfs_trips'
                                    ]) RETURNS text AS $$
DECLARE
	gtfs_table text;
    col_name text;
    col_row RECORD;
    sql_cols text;
    excl text[];
    command text;
    address text;
BEGIN

	-- Specify columns to exclude from export (table.column) 
	excl = ARRAY['gtfs_stops.stop_street',
                 'gtfs_stops.stop_city',
                 'gtfs_stops.stop_region',
                 'gtfs_stops.stop_postcode',
                 'gtfs_stops.stop_country',
                 'gtfs_stops.direction',
                 'gtfs_stops.position',
				 'gtfs_feed_info.feed_timezone',
				 'agency.agency_email',
				 'agency.the_geom',
				 'agency.to_route_id',
				 'agency.service_id',
				 'agency.from_route_id'
				 ];
               
    -- Initialise string to hold command
	command = '';

	-- Loop through each table
	FOREACH gtfs_table IN ARRAY gtfs_tables
    LOOP

        sql_cols = '';
    
        --RAISE NOTICE '%', gtfs_table;
		
        FOR col_row IN
            SELECT
            	column_name
            FROM information_schema.columns
            WHERE table_name = gtfs_table
            ORDER BY ordinal_position
        LOOP
        
            IF NOT ARRAY[(gtfs_table || '.' || col_row.column_name)] && excl THEN
        
                IF sql_cols  = '' THEN
                    sql_cols = col_row.column_name;
                ELSE
                    sql_cols = sql_cols || ', '  || col_row.column_name;
                END IF;
                
            END IF;
        
        END LOOP;
    
    	command = command || '\copy (SELECT ' || sql_cols || ' FROM ' || gtfs_table || ') TO ./' || substring(gtfs_table, 6) || '.txt WITH CSV HEADER;' || chr(10);
    
    END LOOP;

	RAISE NOTICE 'Copy script to bash in linux and run';

    SELECT SUBSTRING((SELECT inet_server_addr()::text), '\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}')
        INTO address;
    
    RETURN 	'mkdir ' || (SELECT current_database()) || chr(10) || 
    		'cd ' || (SELECT current_database()) || chr(10) || 
    		' psql -h ' || address || ' -p ' || (SELECT inet_server_port()) || ' -U ' || (SELECT current_user)   || ' -d ' || (SELECT current_database())  || ' << EOF ' || chr(10) || 
            command || chr(10) || 
            'EOF';
    
END;
$$ LANGUAGE plpgsql;

-- Generate script, copy and paste this elsewhere to run
SELECT * FROM my_export_tables();



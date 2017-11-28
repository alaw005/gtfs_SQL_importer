    
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
    formated_text text;
BEGIN

	-- Specify columns to exclude from export (table.column) 
	excl = ARRAY['gtfs_agency.agency_email',
				 'gtfs_agency.the_geom',
                 'gtfs_fare_rules.service_id',
                 'gtfs_feed_info.feed_id',
                 'gtfs_feed_info.feed_contact_email',
                 'gtfs_feed_info.feed_contact_url',
				 'gtfs_feed_info.feed_timezone',
                 'gtfs_frequencies.start_time_seconds',
                 'gtfs_frequencies.end_time_seconds',
                 'gtfs_stops.stop_street',
                 'gtfs_stops.stop_city',
                 'gtfs_stops.stop_region',
                 'gtfs_stops.stop_postcode',
                 'gtfs_stops.stop_country',
                 'gtfs_stops.direction',
                 'gtfs_stops.position',
                 'gtfs_stops.the_geom',
                 'gtfs_stop_times.arrival_time_seconds',
                 'gtfs_stop_times.departure_time_seconds',
                 'gtfs_transfers.from_route_id',
                 'gtfs_transfers.to_route_id',
                 'gtfs_transfers.service_id',
                 'gtfs_trips.trip_type'
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
            
                IF col_row.column_name = ANY ( ARRAY['start_date', 'end_date', 'date']) THEN
                    formated_text = 'to_char(' || col_row.column_name || ', ''YYYYMMDD'') AS ' || col_row.column_name;
                ELSE
                    formated_text = col_row.column_name;
                END IF;
                
                IF sql_cols  = '' THEN
                    sql_cols = formated_text;
                ELSE
                    sql_cols = sql_cols || ', '  || formated_text;
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


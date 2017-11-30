/*
	Required GTFS tables (do a find and replace if your database has different table names):
		gtfs_stop_times
*/

/*
    Generate arrival_time and departure_time for GTFS stop_times. Requires shape_dist_traveled
    to have been generated first in order to interpolate times.
	
	Function:
		my_gtfs_calculate_stop_times()

	Required GTFS tables:
		gtfs_stop_times
				
	Author:
		Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS public.my_gtfs_calculate_stop_times(boolean);
CREATE OR REPLACE FUNCTION public.my_gtfs_calculate_stop_times(set_timepoints boolean DEFAULT True) RETURNS integer AS $$
DECLARE

    my_current_row RECORD;
    myprevious_time text;
    myprevious_dist text;
    mynext_time text;
    mynext_dist text;
    
    my_segment_metres integer;
    my_segment_seconds integer;
    my_seconds_per_metre float(8);
     
    my_current_trip_id text;
    my_current_stop_time time;
	my_timepoint_flag integer;
    
BEGIN

    RAISE NOTICE 'Starting...';
    
    /*
    CREATE INDEX gtfs_stop_times_trip_id_idx ON gtfs_stop_times (trip_id);
    CREATE INDEX gtfs_stop_times_stop_sequence_idx ON gtfs_stop_times (stop_sequence);
    */

	-- Set existing times as timepoints (NB: This may not be valid, if only filling
    -- in some gaps). The test for timepoint <> 0 is to ensure any stops explicity
    -- identified as not being timepoints (i.e. 0 not null) do not become timepoints 
    IF set_timepoints THEN
	    UPDATE gtfs_stop_times SET
    	    timepoint = 1
    	WHERE arrival_time IS NOT NULL AND timepoint IS NULL;
	END IF;
    
    -- Initialise trip
    my_current_trip_id = '';
    
    -- Loop through each stop in trip_stop_times in order so can match
    -- to shape
 	FOR my_current_row IN 
        SELECT
            *,
            shape_dist_traveled - LAG(shape_dist_traveled, 1) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS inc_distance
        FROM gtfs_stop_times
        ORDER BY trip_id, stop_sequence
    LOOP
    	
        --RAISE NOTICE 'Importing trip #% - %', my_current_row.trip_id, my_current_row.stop_sequence;
    
    	IF my_current_trip_id <> my_current_row.trip_id THEN
        	my_current_trip_id = my_current_row.trip_id;
            my_current_stop_time = my_current_row.arrival_time::time;           
        END IF;
    
    	-- Get previous timing point
    	SELECT
        	departure_time,
            shape_dist_traveled
  		INTO
        	myprevious_time, 
            myprevious_dist
        FROM gtfs_stop_times
		WHERE departure_time IS NOT NULL
                AND trip_id = my_current_row.trip_id 
                AND stop_sequence <= my_current_row.stop_sequence 
        ORDER BY stop_sequence DESC 
        LIMIT 1;

    	-- Get next timing point
    	SELECT
        	arrival_time,
            shape_dist_traveled
  		INTO
        	mynext_time,
            mynext_dist
        FROM gtfs_stop_times
		WHERE arrival_time IS NOT NULL
                AND trip_id = my_current_row.trip_id 
                AND stop_sequence >= my_current_row.stop_sequence 
        ORDER BY stop_sequence ASC 
        LIMIT 1;
       
        -- Calculations
        my_segment_metres = mynext_dist::integer - myprevious_dist::integer;
        my_segment_seconds = EXTRACT(EPOCH FROM mynext_time::time - myprevious_time::time);
        my_seconds_per_metre = CASE WHEN my_segment_metres = 0 THEN NULL ELSE my_segment_seconds::float(8) / my_segment_metres::float(8) END;
		
        -- Determine time and timepoint flag
        IF my_current_row.departure_time IS NOT NULL THEN
        	my_current_stop_time = my_current_row.departure_time::time;
            my_timepoint_flag = my_current_row.timepoint;
        ELSE
			my_current_stop_time = (round((EXTRACT(EPOCH FROM my_current_stop_time) + (my_current_row.inc_distance * my_seconds_per_metre)) / 60 ) * interval '1 minute')::time;
            my_timepoint_flag = 0; /* explicity state that approximate */
        END IF;
     
        -- Saving results (not only updating records where arrival_time is null)
        UPDATE gtfs_stop_times SET
            arrival_time = my_current_stop_time,
            departure_time = my_current_stop_time,
            timepoint = my_timepoint_flag
        WHERE arrival_time IS NULL
        	AND trip_id = my_current_row.trip_id
            AND stop_sequence = my_current_row.stop_sequence;
        
    END LOOP;
   
    RAISE NOTICE 'Finished.';
    RETURN 1;

END;
$$ LANGUAGE plpgsql;

-- Execute function
SELECT FROM my_gtfs_calculate_stop_times(False);


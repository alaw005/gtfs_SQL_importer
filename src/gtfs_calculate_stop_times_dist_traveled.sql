
/*
    Generate distance travelled data for GTFS [stop_times] table  

	Function:
        my_gtfs_calculate_stop_times_dist_traveled()

	Required GTFS tables:
		gtfs_shapes, gtfs_trips, gtfs_stop_times, gtfs_stops
		
	Author:
		Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS public.my_gtfs_calculate_stop_times_dist_traveled();
CREATE OR REPLACE FUNCTION public.my_gtfs_calculate_stop_times_dist_traveled() RETURNS integer AS $$
DECLARE

    my_current_trip RECORD;
    my_trip_id text;
    my_shape_pt_sequence integer;
    my_previous_shape_pt_sequence integer;
    my_segment_distance integer;
	
BEGIN

    RAISE NOTICE 'Starting...';

	/*
		Initialise tables for matching
	*/ 
	
	RAISE NOTICE 'Creating edges table for network routing';

	DROP TABLE IF EXISTS tmp_edges;
	CREATE  TABLE tmp_edges AS 
		SELECT
			row_number() OVER ()::integer -1 AS id,
			tmp_nodes.shape_id,
			tmp_nodes.shape_pt_sequence,
			LAG(tmp_nodes.id, 1) OVER w AS source,
			tmp_nodes.id AS target,
			ST_Distance(LAG(tmp_nodes.the_geom, 1) OVER w, tmp_nodes.the_geom) AS cost,
			ST_MakeLine(LAG(tmp_nodes.the_geom, 1) OVER w, tmp_nodes.the_geom) AS the_geom
		FROM (SELECT
					row_number() OVER ()::integer AS id,
					gtfs_shapes.shape_id::text AS shape_id,
					gtfs_shapes.shape_pt_sequence::integer AS shape_pt_sequence,
					ST_Transform(ST_SetSRID(ST_MakePoint(gtfs_shapes.shape_pt_lon, gtfs_shapes.shape_pt_lat), 4326), 2193) AS the_geom
				FROM gtfs_shapes
				ORDER BY shape_id, shape_pt_sequence) AS tmp_nodes 
		WINDOW w AS (PARTITION BY tmp_nodes.shape_id ORDER BY tmp_nodes.shape_id, tmp_nodes.shape_pt_sequence)
		ORDER BY tmp_nodes.shape_id, tmp_nodes.shape_pt_sequence;
	DELETE FROM tmp_edges WHERE source IS NULL; /* Remove incomplete edges */
	ALTER TABLE ONLY tmp_edges ADD CONSTRAINT pk_tmp_edges PRIMARY KEY(id);
	CREATE INDEX tmp_edges_idx ON tmp_edges (shape_id, shape_pt_sequence);
	CREATE INDEX tmp_edges_shape_idx ON tmp_edges (shape_id);
	CREATE INDEX tmp_edges_geom_gix ON tmp_edges USING GIST (the_geom);
   
	RAISE NOTICE 'Creating trip shapes for bus stop coordinate snapping';

	-- NB: This is to ensure bus stop coorinates are along the relevant shape,
	-- but may not actually be necessary as matching query below now matches	
	-- stops to the nearest shape.
	DROP TABLE IF EXISTS tmp_trips;
	CREATE  TABLE tmp_trips AS
		SELECT
			row_number() OVER ()::integer AS id,
			gtfs_trips.trip_id,
			gtfs_trips.route_id,
			gtfs_trips.shape_id,
			NULL::integer AS shape_length,
			ST_MakeLine(ST_Transform(ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326), 2193))::geometry(LineString,2193) AS the_geom
		FROM gtfs_trips
			LEFT JOIN gtfs_shapes ON gtfs_shapes.shape_id = gtfs_trips.shape_id
		GROUP BY 
			gtfs_trips.route_id,
			gtfs_trips.shape_id,
			gtfs_trips.trip_id;
	ALTER TABLE ONLY tmp_trips ADD CONSTRAINT pk_tmp_trips PRIMARY KEY(id);
	CREATE INDEX tmp_trips_trip_idx ON tmp_trips (trip_id);
	CREATE INDEX tmp_trips_geom_gix ON tmp_trips USING GIST (the_geom);
	UPDATE tmp_trips SET shape_length = ST_Length(the_geom);

	RAISE NOTICE 'Creating stop times with bus stop coordinates snapped to shape';

	DROP TABLE IF EXISTS tmp_stop_times;
	CREATE  TABLE tmp_stop_times AS
		SELECT 
			row_number() OVER ()::integer AS id,
			gtfs_stop_times.trip_id,
			gtfs_stop_times.stop_sequence,
			gtfs_stop_times.stop_id,
			gtfs_stops.stop_name,
            ST_Transform(ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326), 2193)::geometry(Point,2193) AS the_geom,
			tmp_trips.shape_id,
            NULL::integer AS shape_pt_sequence,
            NULL::integer AS cumul_dist_traveled
		FROM gtfs_stop_times
			LEFT JOIN gtfs_stops ON gtfs_stops.stop_id = gtfs_stop_times.stop_id
			LEFT JOIN tmp_trips ON tmp_trips.trip_id = gtfs_stop_times.trip_id
		ORDER BY trip_id, stop_sequence;
	ALTER TABLE ONLY tmp_stop_times ADD CONSTRAINT pk_tmp_stop_times PRIMARY KEY(id);
	CREATE INDEX tmp_stop_times_idx ON tmp_stop_times (trip_id);
	CREATE INDEX tmp_stop_times_geom_gix ON tmp_stop_times USING GIST (the_geom);

	/*
        Match to shape
	*/

	RAISE NOTICE 'Start matching ...';
    
	-- Initialise variable for tracking current trip_id so can start
    -- matching from beginning of shape for each trip_id
	my_trip_id = '';
    
	-- Loop through each stop in trip_stop_times in order so can match
    -- to shape
 	FOR my_current_trip IN 
        SELECT
            id,
            trip_id,
            stop_sequence,
            the_geom,
            shape_id
        FROM tmp_stop_times
        ORDER BY trip_id, stop_sequence
    LOOP

		-- Reset sequence to start of shape sequence
		IF my_trip_id <> my_current_trip.trip_id THEN
			my_trip_id = my_current_trip.trip_id;
		    my_previous_shape_pt_sequence = (SELECT Min(shape_pt_sequence) FROM tmp_edges WHERE shape_id = my_current_trip.shape_id );
            RAISE NOTICE 'Importing trip #%s', my_trip_id;
        END IF;
        
        -- Locate next point in shape sequence and distance along the last shape segment
		SELECT
        	shape_pt_sequence,
            COALESCE(tmp_edges.cost * ST_LineLocatePoint(tmp_edges.the_geom, my_current_trip.the_geom), 0)
        INTO 
        	my_shape_pt_sequence, my_segment_distance
        FROM tmp_edges
        WHERE tmp_edges.shape_id = my_current_trip.shape_id 
            AND tmp_edges.shape_pt_sequence::integer > my_previous_shape_pt_sequence
            AND ST_DWithin(tmp_edges.the_geom, my_current_trip.the_geom, 200) /* Search within 200m */
        ORDER BY 
			tmp_edges.shape_id,
            ST_Distance(tmp_edges.the_geom, my_current_trip.the_geom), /* Get closest first */
            tmp_edges.shape_pt_sequence::integer /* Get the first point in sequence within 200m */
        LIMIT 1;
        
        -- Calculate cumulative distance
        UPDATE tmp_stop_times SET 
        	shape_pt_sequence = my_shape_pt_sequence,
        	cumul_dist_traveled = (SELECT 
                                       		COALESCE(SUM(COST),0) AS distance
                                       FROM tmp_edges
                                       WHERE shape_id = my_current_trip.shape_id AND shape_pt_sequence < my_shape_pt_sequence) + my_segment_distance
        WHERE 
        	tmp_stop_times.id = my_current_trip.id;

		-- Update previous shape_pt_sequence for reference in next loop
        my_previous_shape_pt_sequence = my_shape_pt_sequence ;

    END LOOP;

	RAISE NOTICE 'Remove offsets between first stop and start of shape';
	
	UPDATE tmp_stop_times SET
    	cumul_dist_traveled = cumul_dist_traveled - a.min_dist
	FROM (SELECT 
                trip_id,
                Min(cumul_dist_traveled) AS min_dist
            FROM tmp_stop_times
            GROUP BY trip_id) AS a
	WHERE tmp_stop_times.trip_id = a.trip_id;
	
	RAISE NOTICE 'Updating stop_times with results of calculation';
	
    UPDATE gtfs_stop_times
    	SET shape_dist_traveled = a.cumul_dist_traveled
    FROM tmp_stop_times AS a
    WHERE a.trip_id = gtfs_stop_times.trip_id AND a.stop_sequence = gtfs_stop_times.stop_sequence;
    
    
    RAISE NOTICE 'Finished.';
    RETURN 1;


END;
$$ LANGUAGE plpgsql;

-- Execute function
SELECT FROM my_gtfs_calculate_stop_times_dist_traveled();


/*
    Generate distance travelled data for GTFS [stop_times] table  

	Function:
        my_gtfs_calculate_stop_times_dist_traveled()

	Required GTFS tables:
		gtfs_shapes, gtfs_trips, gtfs_stop_times, gtfs_stops
		
	Author:
		Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS my_gtfs_calculate_stop_times_dist_traveled();
CREATE OR REPLACE FUNCTION my_gtfs_calculate_stop_times_dist_traveled() RETURNS integer AS $$
DECLARE
    trip_row RECORD;
    my_trip text;
	my_edge_id integer;
	my_previous_edge_id integer;
    my_edge_length float(8);
    my_distance float(8);
BEGIN

    RAISE NOTICE 'Starting...';

	/*
		Initialise tables for matching
	*/ 
	
	RAISE NOTICE 'Creating edges table for network routing';
	
	CREATE TEMP TABLE tmp_edges AS 
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
	CREATE TEMP TABLE tmp_trips AS
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

	CREATE TEMP TABLE tmp_stop_times AS
		SELECT 
			row_number() OVER ()::integer AS id,
			gtfs_stop_times.trip_id,
			tmp_trips.route_id,
			tmp_trips.shape_id,
			gtfs_stop_times.stop_sequence,
			gtfs_stop_times.stop_id,
			gtfs_stops.stop_name,
			ST_ClosestPoint(
					tmp_trips.the_geom,
					ST_Transform(ST_SetSRID(ST_MakePoint(stop_lon, stop_lat), 4326), 2193)
				)::geometry(Point,2193) AS the_geom,
			NULL::float(8) AS cumul_dist_traveled,
			NULL::integer AS edge_id,
			NULL::float(8) AS edge_length,
			NULL::float(8) AS edge_proportion,
			NULL::float(8) AS edge_dist_traveled
		FROM gtfs_stop_times
			LEFT JOIN gtfs_stops ON gtfs_stops.stop_id = gtfs_stop_times.stop_id
			LEFT JOIN tmp_trips ON tmp_trips.trip_id = gtfs_stop_times.trip_id
		ORDER BY trip_id, stop_sequence;
	ALTER TABLE ONLY tmp_stop_times ADD CONSTRAINT pk_tmp_stop_times PRIMARY KEY(id);
	CREATE INDEX tmp_stop_times_idx ON tmp_stop_times (trip_id);
	CREATE INDEX tmp_stop_times_geom_gix ON tmp_stop_times USING GIST (the_geom);

	/*
        Match using network edges
	*/

	RAISE NOTICE 'Start matching ...';
    
	-- Initialise variable for tracking current trip_id so can start
    -- matching from beginning of shape for each trip_id
	my_trip = '';
    
	-- Loop through each stop in trip_stop_times in order so can match
    -- to shape
 	FOR trip_row IN 
        SELECT
            id,
            shape_id,
            trip_id,
            stop_sequence,
            the_geom,
            edge_id,
            edge_length,
            edge_proportion
        FROM tmp_stop_times
        ORDER BY trip_id, stop_sequence
    LOOP

		-- Reset edge matching when move to next trip
		IF my_trip <> trip_row.trip_id THEN 
		    my_previous_edge_id = 0;
            RAISE NOTICE 'Importing trip #%s', my_trip;
        END IF;
        my_trip = trip_row.trip_id;
        
        -- Locate next edge_id
		SELECT
            tmp_edges.id,
            tmp_edges.cost,
            ST_LineLocatePoint(tmp_edges.the_geom, trip_row.the_geom)
        INTO 
        	my_edge_id, my_edge_length, my_distance
        FROM tmp_edges
        WHERE tmp_edges.shape_id = trip_row.shape_id 
            AND tmp_edges.id >= my_previous_edge_id
            AND ST_DWithin(tmp_edges.the_geom, trip_row.the_geom, 100) /* Search within 100m */
        ORDER BY 
            ST_Distance(tmp_edges.the_geom, trip_row.the_geom) /* Get closest match within 100m */
        LIMIT 1;

		-- update edge_id in stops table
		UPDATE tmp_stop_times SET
        	edge_id = my_edge_id,
            edge_length = my_edge_length,
            edge_proportion = my_distance,
            edge_dist_traveled = (my_edge_length * my_distance)::integer
        WHERE tmp_stop_times.id = trip_row.id;

		-- Update previous edge for next loop
        my_previous_edge_id = my_edge_id;

    END LOOP;

	RAISE NOTICE 'Matching complete ... running final calculations';
    
    RAISE NOTICE 'Calculating cumulative distance to nearest metre';
	
    UPDATE tmp_stop_times AS a SET
        /* Sum from beginning of shape, not from first matched edge */
    	cumul_dist_traveled = ((SELECT COALESCE(SUM(cost),0) FROM tmp_edges WHERE tmp_edges.shape_id = a.shape_id AND tmp_edges.id <= b.previous_edge) + b.edge_dist_traveled)::integer
    FROM (SELECT
                id,
                FIRST_VALUE(edge_id) OVER w AS first_edge,
                LAST_VALUE(edge_id-1) OVER w AS previous_edge,
          		edge_dist_traveled
            FROM tmp_stop_times
            WINDOW w AS (PARTITION BY trip_id ORDER BY stop_sequence)) AS b
	WHERE a.id = b.id;

	RAISE NOTICE 'Updating stop_times with cumulative distance calculation';
	
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


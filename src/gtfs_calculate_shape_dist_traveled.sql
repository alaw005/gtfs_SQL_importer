/*
	Required GTFS tables (do a find and replace if your database has different table names):
		gtfs_shapes, gtfs_trips, gtfs_stop_times, gtfs_stops
*/

/*
    Generate distance travelled data for GTFS shapes file
	
	Function:
		my_gtfs_calculate_shape_dist_traveled()

	Required GTFS tables:
		gtfs_shapes
				
	Author:
		Adam Lawrence <alaw005@gmail.com>
*/

DROP FUNCTION IF EXISTS public.my_gtfs_calculate_shape_dist_traveled();
CREATE OR REPLACE FUNCTION public.my_gtfs_calculate_shape_dist_traveled() RETURNS integer AS $$
DECLARE
BEGIN

    RAISE NOTICE 'Starting...';

    RAISE NOTICE 'Updating shape_dist_traveled in gtfs_shapes';
	
    UPDATE gtfs_shapes SET
        shape_dist_traveled = b.shape_dist_traveled
    FROM (SELECT /* cumulative sum of segment lengths */
                shape_id, 
                shape_pt_sequence,
                SUM(dist) OVER (PARTITION BY shape_id ORDER BY shape_pt_sequence) AS shape_dist_traveled
            FROM (SELECT /* Calculate segment lengths */
                        shape_id, 
                        shape_pt_sequence,
                        COALESCE(ST_Length(ST_Transform(ST_SetSRID(ST_MakeLine(ST_MakePoint(shape_pt_lon, shape_pt_lat), ST_MakePoint(LAG(shape_pt_lon, 1) OVER w, LAG(shape_pt_lat, 1) OVER w)), 4326), 2193)), 0)::integer AS dist
                    FROM gtfs_shapes
                    WINDOW w AS (PARTITION BY shape_id ORDER BY shape_pt_sequence)) AS a) AS b
    WHERE gtfs_shapes.shape_id = b.shape_id AND gtfs_shapes.shape_pt_sequence = b.shape_pt_sequence;

    RAISE NOTICE 'Finished.';
    RETURN 1;
    
END;
$$ LANGUAGE plpgsql;

-- Execute function
SELECT FROM my_gtfs_calculate_shape_dist_traveled();



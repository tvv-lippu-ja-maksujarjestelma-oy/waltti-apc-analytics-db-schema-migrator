CREATE SCHEMA util;
COMMENT ON SCHEMA
  util IS
  'General utilities';

CREATE FUNCTION util.convert_operating_timestamp_to_timestamptz(
  IN operating_date date,
  IN operating_time interval HOUR TO SECOND,
  IN timezone_name text
)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
RETURNS NULL ON NULL INPUT
PARALLEL SAFE
AS $util_convert_operating_timestamp_to_timestamptz$
  WITH noon_on_operating_date AS (
    SELECT
      concat(
        to_char(operating_date, 'YYYY-MM-DD'),
        'T12:00:00 ',
        timezone_name
      )::timestamptz AS noon
  )
  SELECT noon + operating_time - 'PT12:00:00'::interval HOUR TO SECOND
  FROM noon_on_operating_date
$util_convert_operating_timestamp_to_timestamptz$;
COMMENT ON FUNCTION
  util.convert_operating_timestamp_to_timestamptz IS
  'Convert operating date and time to timestamptz. As operating time can be '
  'over 24 hours, e.g. 28:34:51, we cannot use the data type time. The '
  'conversion is based on expecting the DST change to be over by noon. All '
  'operating times in an operating date that contains a DST change are '
  'interpreted to happen after the DST change, e.g. the combination of the '
  'operating date 2021-10-31 and the operating time 02:00:00 in the timezone '
  'Europe/Helsinki corresponds with the moment 2021-10-31T00:00:00Z and not '
  '2021-10-30T23:00:00Z like usually.';



CREATE SCHEMA apc_gtfs;
COMMENT ON SCHEMA
  apc_gtfs IS
  'GTFS reference data';

-- FIXME: Consider the concept of "feed" from GTFS as a column in stops, routes
-- and trips. It could look like this:
-- table feed: (unique_feed_publisher_id, feed_publisher_id, feed_version)

CREATE TABLE apc_gtfs.feed_publisher (
  unique_feed_publisher_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  feed_publisher_id text NOT NULL UNIQUE
);

CREATE TABLE apc_gtfs.stop (
  unique_stop_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  unique_feed_publisher_id uuid
    NOT NULL
    REFERENCES apc_gtfs.feed_publisher (unique_feed_publisher_id),
  stop_id text NOT NULL,
  timezone_name text NOT NULL,
  CONSTRAINT gtfs_unique_feed_publisher_id_with_stop_id_must_be_unique
    UNIQUE (unique_feed_publisher_id, stop_id)
  -- FIXME: add constraint: timezone_name must match `pg_timezone_name.name`.
  --        pg_timezone_name is a view so `REFERENCES` cannot be used.
);
CREATE INDEX ON
  apc_gtfs.stop
  (unique_feed_publisher_id);

CREATE TABLE apc_gtfs.route (
  unique_route_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  unique_feed_publisher_id uuid
    NOT NULL
    REFERENCES apc_gtfs.feed_publisher (unique_feed_publisher_id),
  route_id text NOT NULL,
  CONSTRAINT gtfs_unique_feed_publisher_id_with_route_id_must_be_unique
    UNIQUE (unique_feed_publisher_id, route_id)
);
CREATE INDEX ON
  apc_gtfs.route
  (unique_feed_publisher_id);

-- FIXME: Should we separate tables trip (in GTFS) and dated_trip (not in GTFS)?
CREATE TABLE apc_gtfs.trip (
  unique_trip_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  unique_feed_publisher_id uuid
    NOT NULL
    REFERENCES apc_gtfs.feed_publisher (unique_feed_publisher_id),
  trip_id text NOT NULL,
  start_operating_date date NOT NULL,
  start_operating_time interval HOUR TO SECOND NOT NULL,
  unique_route_id uuid NOT NULL REFERENCES apc_gtfs.route (unique_route_id),
  -- One could use boolean as direction_id has to be either 0 or 1. To ease
  -- matching and to reduce mistakes, use a numeric type instead.
  direction_id smallint NOT NULL,
  CONSTRAINT gtfs_publisher_id_with_trip_id_with_start_moment_must_be_unique
    UNIQUE (
      unique_feed_publisher_id,
      trip_id,
      start_operating_date,
      start_operating_time
    ),
  CONSTRAINT gtfs_direction_id_must_be_0_or_1
    CHECK (direction_id = 0 OR direction_id = 1)
);
CREATE INDEX ON
  apc_gtfs.trip
  (unique_feed_publisher_id);
CREATE INDEX ON
  apc_gtfs.trip
  (unique_route_id);
COMMENT ON TABLE
  apc_gtfs.trip IS
  'Unique trips, i.e. matches with the concept of DatedVehicleJourney in '
  'Transmodel. References: https://gtfs.org/schedule/reference/#tripstxt and '
  'https://gtfs.org/realtime/reference/#message-tripdescriptor';

-- FIXME: unique_stop_visit_id or just stop_visit_id? This one is not in GTFS.
CREATE TABLE apc_gtfs.stop_visit (
  unique_stop_visit_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  unique_trip_id uuid NOT NULL REFERENCES apc_gtfs.trip (unique_trip_id),
  unique_stop_id uuid NOT NULL REFERENCES apc_gtfs.stop (unique_stop_id),
  stop_sequence smallint NOT NULL,
  CONSTRAINT gtfs_unique_trip_id_with_stop_sequence_must_be_unique
    UNIQUE (unique_trip_id, stop_sequence),
  CONSTRAINT gtfs_stop_sequence_must_be_nonnegative CHECK (stop_sequence >= 0)
);
COMMENT ON TABLE
  apc_gtfs.stop_visit IS
  'Each visit on or passing by a stop for each unique trip, i.e. similar to '
  'the concept of Departure in Transmodel.';

-- FIXME: consider select and insert instead.
-- The only reason to UPDATE on conflict is to ensure a return value.
CREATE FUNCTION apc_gtfs.upsert_stop_visit(
  IN feed_publisher_id text,
  IN stop_id text,
  IN timezone_name text,
  IN route_id text,
  IN trip_id text,
  IN start_operating_date date,
  IN start_operating_time interval HOUR TO SECOND,
  IN direction_id smallint,
  IN stop_sequence smallint,
  -- FIXME: remove OUT and test client code. anyway, maybe this should be in
  -- the typescript client
  OUT unique_stop_visit_id uuid
)
RETURNS uuid
LANGUAGE sql
VOLATILE
RETURNS NULL ON NULL INPUT
PARALLEL UNSAFE
AS $apc_gtfs_upsert_stop_visit$
  WITH feed_publisher AS (
    INSERT INTO apc_gtfs.feed_publisher (feed_publisher_id)
    VALUES ($1)
    ON CONFLICT (feed_publisher_id) DO UPDATE SET
      feed_publisher_id = EXCLUDED.feed_publisher_id
    RETURNING unique_feed_publisher_id
  ), stop AS (
    INSERT INTO apc_gtfs.stop (unique_feed_publisher_id, stop_id, timezone_name)
    (SELECT fp.unique_feed_publisher_id, $2, $3 FROM feed_publisher AS fp)
    ON CONFLICT (unique_feed_publisher_id, stop_id) DO UPDATE SET
      timezone_name = EXCLUDED.timezone_name
    RETURNING unique_stop_id, unique_feed_publisher_id
  ), route AS (
    INSERT INTO apc_gtfs.route (unique_feed_publisher_id, route_id)
    (SELECT fp.unique_feed_publisher_id, $4 FROM feed_publisher AS fp)
    ON CONFLICT (unique_feed_publisher_id, route_id) DO UPDATE SET
      route_id = EXCLUDED.route_id
    RETURNING unique_route_id, unique_feed_publisher_id
  ), trip AS (
    INSERT INTO apc_gtfs.trip (
      unique_feed_publisher_id,
      trip_id,
      start_operating_date,
      start_operating_time,
      unique_route_id,
      direction_id
    )
    (SELECT fp.unique_feed_publisher_id, $5, $6, $7, r.unique_route_id, $8
      FROM feed_publisher AS fp
        INNER JOIN route AS r
          ON (r.unique_feed_publisher_id = fp.unique_feed_publisher_id)
    )
    ON CONFLICT (
      unique_feed_publisher_id,
      trip_id,
      start_operating_date,
      start_operating_time
    ) DO UPDATE SET
      unique_route_id = EXCLUDED.unique_route_id,
      direction_id = EXCLUDED.direction_id
    RETURNING unique_trip_id, unique_feed_publisher_id
  )
  INSERT INTO apc_gtfs.stop_visit (
    unique_trip_id,
    unique_stop_id,
    stop_sequence
  )
  (SELECT t.unique_trip_id, s.unique_stop_id, $9
    FROM trip AS t
      INNER JOIN stop AS s
        ON (s.unique_feed_publisher_id = t.unique_feed_publisher_id)
  )
  ON CONFLICT (unique_trip_id, stop_sequence) DO UPDATE SET
    unique_stop_id = EXCLUDED.unique_stop_id
  RETURNING unique_stop_visit_id
$apc_gtfs_upsert_stop_visit$;



CREATE SCHEMA apc_occupancy;
COMMENT ON SCHEMA
  apc_occupancy IS
  'APC analytics data';

CREATE TABLE apc_occupancy.count_class (
  count_class text PRIMARY KEY
);

INSERT INTO apc_occupancy.count_class
  (count_class)
  VALUES
  ('adult'),
  ('child'),
  ('pram'),
  ('bike'),
  ('wheelchair'),
  ('other');

CREATE TABLE apc_occupancy.count_quality (
  count_quality text PRIMARY KEY
);

INSERT INTO apc_occupancy.count_quality
  (count_quality)
  VALUES
  ('regular'),
  ('defect'),
  ('other');

CREATE TABLE apc_occupancy.counting_vendor (
  counting_vendor_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  name text NOT NULL UNIQUE
);

CREATE TABLE apc_occupancy.door_count (
  door_count_id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  counting_vendor_id uuid
    NOT NULL
    REFERENCES apc_occupancy.counting_vendor (counting_vendor_id),
  unique_stop_visit_id uuid
    NOT NULL
    REFERENCES apc_gtfs.stop_visit (unique_stop_visit_id),
  count_quality text
    NOT NULL
    REFERENCES apc_occupancy.count_quality (count_quality),
  door_name text NOT NULL,
  count_class text NOT NULL REFERENCES apc_occupancy.count_class (count_class),
  door_count_in smallint NOT NULL,
  door_count_out smallint NOT NULL,
  CONSTRAINT apc_occupancy_door_count_uniqueness
    UNIQUE (
      counting_vendor_id,
      unique_stop_visit_id,
      door_name,
      count_class
    ),
  CONSTRAINT apc_occupancy_door_counts_must_be_nonnegative
    CHECK (door_count_in >= 0 AND door_count_out >= 0)
);
CREATE INDEX ON
  apc_occupancy.door_count
  (counting_vendor_id);
CREATE INDEX ON
  apc_occupancy.door_count
  (unique_stop_visit_id);
CREATE INDEX ON
  apc_occupancy.door_count
  (count_quality);
CREATE INDEX ON
  apc_occupancy.door_count
  (count_class);

CREATE FUNCTION apc_occupancy.upsert_door_count(
  IN unique_stop_visit_id uuid,
  IN counting_vendor_name text,
  IN count_quality text,
  IN door_name text,
  IN count_class text,
  IN count_door_in smallint,
  IN count_door_out smallint,
  OUT door_count_id uuid
)
RETURNS uuid
LANGUAGE sql
VOLATILE
RETURNS NULL ON NULL INPUT
PARALLEL UNSAFE
AS $apc_occupancy_upsert_door_counts$
  WITH counting_vendor AS (
    INSERT INTO apc_occupancy.counting_vendor (name)
    VALUES ($2)
    ON CONFLICT (name) DO UPDATE SET
      name = EXCLUDED.name
    RETURNING counting_vendor_id
  )
  INSERT INTO apc_occupancy.door_count (
    counting_vendor_id,
    unique_stop_visit_id,
    count_quality,
    door_name,
    count_class,
    door_count_in,
    door_count_out
  )
  (SELECT
    cv.counting_vendor_id,
    sv.unique_stop_visit_id,
    $3,
    $4,
    $5,
    $6,
    $7
    FROM counting_vendor AS cv
      CROSS JOIN apc_gtfs.stop_visit AS sv
    WHERE sv.unique_stop_visit_id = $1
  )
  ON CONFLICT (
    counting_vendor_id,
    unique_stop_visit_id,
    door_name,
    count_class
  ) DO UPDATE SET
    -- FIXME:
    -- These changes are enough if the upstream sends messages with zero counts
    -- for whatever class it sent data for before but will not send in a later
    -- version. E.g. for door 1, if 1 adult in is corrected as 1 child in, the
    -- latter version of the message should also explicitly have 0 adults in.
    --
    -- That limitation should be overcome with two separate queries:
    -- 1) Insert rows and select ids of gtfs_apc.* and
    --    gtfs_occupancy.counting_vendor.
    -- 2) Insert all doors and classes for one vendor, trip and stop into
    --    gtfs_occupancy.door_count after deleting the previous version if it
    --    exists, all in one query.
    --
    -- Also consider how to batch several messages into rows, e.g. every second.
    count_quality = EXCLUDED.count_quality,
    door_count_in = EXCLUDED.door_count_in,
    door_count_out = EXCLUDED.door_count_out
  RETURNING door_count_id;
$apc_occupancy_upsert_door_counts$;

CREATE VIEW apc_occupancy.all_doors_count_by_stop_visit AS (
  WITH distinct_counting_vendors_and_count_classes_per_trip AS (
    SELECT DISTINCT
      sv.unique_trip_id,
      dc.count_class,
      dc.counting_vendor_id,
      cv.name AS counting_vendor_name
    FROM
      apc_occupancy.door_count AS dc
      INNER JOIN apc_occupancy.counting_vendor AS cv
        ON (cv.counting_vendor_id = dc.counting_vendor_id)
      INNER JOIN apc_gtfs.stop_visit AS sv
        ON (sv.unique_stop_visit_id = dc.unique_stop_visit_id)
  ), count_possibilities_per_stop_visit AS (
    SELECT
      combinations.counting_vendor_id,
      combinations.counting_vendor_name,
      combinations.count_class,
      sv.*
    FROM distinct_counting_vendors_and_count_classes_per_trip AS combinations
      INNER JOIN apc_gtfs.stop_visit AS sv
        ON (sv.unique_trip_id = combinations.unique_trip_id)
  ), doors_summed AS (
    SELECT
      fp.feed_publisher_id,
      r.route_id,
      t.direction_id,
      t.start_operating_date AS trip_start_operating_date,
      t.start_operating_time AS trip_start_operating_time,
      util.convert_operating_timestamp_to_timestamptz(
        t.start_operating_date,
        t.start_operating_time,
        s.timezone_name
      ) AS trip_start_moment,
      EXTRACT(isodow FROM t.start_operating_date) AS weekday_number,
      possibilities.stop_sequence,
      s.stop_id,
      possibilities.counting_vendor_name,
      possibilities.count_class,
      sum(COALESCE(dc.door_count_in, 0)) AS count_in,
      sum(COALESCE(dc.door_count_out, 0)) AS count_out,
      -- Helpers not to be shown in the final view.
      s.timezone_name,
      possibilities.counting_vendor_id,
      possibilities.unique_trip_id
    FROM
      apc_occupancy.door_count AS dc
      -- Get all stops, count classes and counting vendors of the trip, even
      -- stops passed by or count classes that had no events for a particular
      -- stop.
      RIGHT OUTER JOIN count_possibilities_per_stop_visit AS possibilities
        ON (
          possibilities.unique_stop_visit_id = dc.unique_stop_visit_id
          AND possibilities.count_class = dc.count_class
          AND possibilities.counting_vendor_id = dc.counting_vendor_id
        )
      INNER JOIN apc_gtfs.trip AS t
        ON (t.unique_trip_id = possibilities.unique_trip_id)
      INNER JOIN apc_gtfs.stop AS s
        ON (s.unique_stop_id = possibilities.unique_stop_id)
      INNER JOIN apc_gtfs.route AS r
        ON (r.unique_route_id = t.unique_route_id)
      INNER JOIN apc_gtfs.feed_publisher AS fp
        ON (s.unique_feed_publisher_id = fp.unique_feed_publisher_id)
    GROUP BY
      fp.feed_publisher_id,
      r.route_id,
      t.direction_id,
      t.start_operating_date,
      t.start_operating_time,
      s.timezone_name,
      possibilities.stop_sequence,
      s.stop_id,
      possibilities.counting_vendor_name,
      possibilities.count_class,
      possibilities.counting_vendor_id,
      possibilities.unique_trip_id
  ), cumulative_counts AS (
    SELECT
      *,
      -- COALESCE was used in doors_summed so there are no NULLs to worry about.
      sum(count_in) OVER w AS cumulative_count_in,
      sum(count_out) OVER w AS cumulative_count_out
    FROM doors_summed
    WINDOW w AS (
      PARTITION BY
        counting_vendor_id,
        unique_trip_id,
        count_class
      ORDER BY stop_sequence
    )
  )
  SELECT
    feed_publisher_id,
    route_id,
    direction_id,
    trip_start_operating_date,
    trip_start_operating_time,
    trip_start_moment,
    (trip_start_moment AT TIME ZONE timezone_name) AS trip_start_moment_local,
    trip_start_moment + ('PT' || stop_sequence - 1 || 'M')::interval
      AS fake_stop_visit_moment,
    weekday_number,
    stop_sequence,
    stop_id,
    counting_vendor_name,
    count_class,
    count_in,
    count_out,
    cumulative_count_in,
    cumulative_count_out,
    -- COALESCE was used in doors_summed so there are no NULLs to worry about.
    cumulative_count_in - cumulative_count_out AS occupancy_after_stop
  FROM
    cumulative_counts
);

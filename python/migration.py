
def create_profiles_table(client):
    try:
        client.execute("""
            CREATE TABLE profiles(
                anonymous_id String,
                num_properties Nested(
                    keys String,
                    vals Float32
                ),
                str_properties Nested(
                    keys String,
                    vals String
                ),
                created_at DateTime
            )ENGINE = MergeTree()
            ORDER BY (anonymous_id, created_at)
        """)
        print('Table was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segments_table(client):
    try:
        client.execute("""
            CREATE TABLE segments(
                segment_id String,
                users Array(String)
            ) Engine = MergeTree()
            ORDER BY (segment_id)
        """)
        print('Table was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_agg_table(client):
    try:
        client.execute("""
            CREATE TABLE segment_agg(
                segment_id String,
                time_stamp DateTime,
                metric_name String,
                metrics_agg Nested(
                    keys String,
                    vals Float32
                )
            )ENGINE = MergeTree()
            PARTITION BY toYYYYMM(time_stamp)
            ORDER BY(segment_id, time_stamp, metric_name)
        """)
        print('Table was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_agg_gender_mv(client):
    try:
        client.execute("""
            CREATE MATERIALIZED VIEW segment_agg_gender_mv
            TO segment_agg
            AS
            SELECT
                segment_id,
                 toStartOfTenMinutes(now()) AS time_stamp,
                'gender' AS metric_name,
                groupArray(gender) AS "metrics_agg.keys",
                groupArray(count) AS "metrics_agg.vals"
            FROM (
                    SELECT
                        segment_id,
                        p.gender,
                        count() AS count
                    FROM segments ARRAY JOIN users
                    JOIN (
                        SELECT
                            anonymous_id,
                            str_properties.vals[indexOf(str_properties.keys, 'gender')] AS gender
                        FROM profiles
                    ) AS p
                    ON (p.anonymous_id  = segments.users)
                    WHERE gender != '' AND gender IS NOT NULL
                    GROUP BY segment_id, gender
                    ORDER BY count DESC, gender
            )
            GROUP BY segment_id, time_stamp
        """)
        print('Materialized view was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_agg_location_mv(client):
    try:
        client.execute("""
            CREATE MATERIALIZED VIEW segment_agg_location_mv
            TO segment_agg
            AS
            SELECT
                segment_id,
                 toStartOfTenMinutes(now()) AS time_stamp,
                'location_city' AS metric_name,
                groupArray(location_city) AS "metrics_agg.keys",
                groupArray(count) AS "metrics_agg.vals"
            FROM (
                SELECT
                    segment_id,
                    location_city,
                    count() AS count
                FROM segments ARRAY JOIN users
                JOIN (
                    SELECT anonymous_id,
                        str_properties.vals[indexOf(str_properties.keys, 'location_city')] AS location_city
                    FROM profiles ) AS p
                ON segments.users = p.anonymous_id
                WHERE location_city != '' AND location_city IS NOT NULL
                GROUP BY segment_id, location_city
                ORDER BY count DESC, location_city
            )
            GROUP BY segment_id, time_stamp
        """)
        print('Materialized view was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_agg_source_mv(client):
    try:
        client.execute("""
            CREATE MATERIALIZED VIEW segment_agg_source_mv
            TO segment_agg
            AS
            SELECT
                segment_id,
                 toStartOfTenMinutes(now()) AS time_stamp,
                'context_campaign_source' AS metric_name,
                groupArray(source) AS "metrics_agg.keys",
                groupArray(count) AS "metrics_agg.vals"
            FROM (
                SELECT
                    segment_id,
                    source,
                    count() AS count
                FROM segments array JOIN users
                JOIN (
                    SELECT
                        anonymous_id,
                        str_properties.vals[indexOf(str_properties.keys, 'context_campaign_source')] AS source
                    FROM profiles ) AS p
                ON segments.users = p.anonymous_id
                WHERE source != '' AND source IS NOT NULL
                GROUP BY segment_id, source
                ORDER BY count DESC, source
            )
            GROUP BY segment_id, time_stamp
        """)
        print('Materialized view was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_last_point_agg_table(client):
    try:
        client.execute("""
            CREATE TABLE segment_last_point_agg(
                segment_id String,
                time_stamp DateTime,
                metric_name String,
                metrics_agg Nested(
                    keys String,
                    vals Float32
                ),
                ack_time DateTime
            ) ENGINE = ReplacingMergeTree(ack_time)
            ORDER BY(segment_id, metric_name)
        """)
        print('Table was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False


def create_segment_last_point_mv(client):
    try:
        client.execute("""
            CREATE MATERIALIZED VIEW segment_last_point_mv
            TO segment_last_point_agg
            AS SELECT
                segment_id,
                time_stamp,
                metric_name,
                metrics_agg.keys,
                metrics_agg.vals,
                now() AS ack_time
            FROM segment_agg
        """)
        print('Materialized view was created success !')
        return True
    except Exception as e:
        print(e.message())
        return False



create_profiles_table(client)
create_segments_table(client)
create_segment_agg_table(client)
create_segment_agg_gender_mv(client)
create_segment_agg_location_mv(client)
create_segment_agg_source_mv(client)
create_segment_last_point_agg_table(client)
create_segment_last_point_mv(client)




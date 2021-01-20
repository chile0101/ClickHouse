# for gender, location, source
def create_categorical_metrics_mv(metric_name, str_key):
    q = f"""
        CREATE MATERIALIZED VIEW segment_agg_{metric_name}_mv TO segment_agg AS
        SELECT
            segment_id,
            now() AS time_stamp,
            '{metric_name}' AS metric_name,
            groupArray(cate) AS `metrics_agg.keys`,
            groupArray(count) AS `metrics_agg.vals`
        FROM
        (
            SELECT
                segment_id,
                p.cate,
                count() AS count
            FROM segments
            ARRAY JOIN users
            INNER JOIN
            (
                SELECT
                    anonymous_id,
                    pf.str_vals[indexOf(pf.str_keys, '{str_key}')] AS cate
                FROM
                (
                    SELECT
                        anonymous_id,
                        argMaxMerge(str_properties.keys) AS str_keys,
                        argMaxMerge(str_properties.vals) AS str_vals
                    FROM user_profile_final
                    GROUP BY
                        tenant_id,
                        anonymous_id
                ) AS pf
            ) AS p ON p.anonymous_id = segments.users
            WHERE (cate != '')
            GROUP BY
                segment_id,
                cate
            ORDER BY
                count DESC,
                cate ASC
        )
        GROUP BY segment_id
    """
    return q


# For revenue, avg.order value, total orders
def create_numerial_metrics_mv(metric_name, num_key):
    q = f"""
        CREATE MATERIALIZED VIEW segment_agg_{metric_name}_mv TO segment_agg AS
        SELECT
            segment_id,
            now() AS time_stamp,
            '{metric_name}' AS metric_name,
            ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
            [max(v), avg(v), median(v), min(v)] AS `metrics_agg.vals`
        FROM
        (
            SELECT *
            FROM segments
            ARRAY JOIN users
        ) AS s
        INNER JOIN
        (
            SELECT
                anonymous_id,
                pf.num_vals[indexOf(pf.num_keys, '{num_key}')] AS v
            FROM
            (
                SELECT
                    anonymous_id,
                    argMaxMerge(num_properties.keys) AS num_keys,
                    argMaxMerge(num_properties.vals) AS num_vals
                FROM user_profile_final
                GROUP BY
                    tenant_id,
                    anonymous_id
            ) AS pf
        ) AS p ON s.users = p.anonymous_id
        WHERE (v > 0)
        GROUP BY segment_id;
        """
    return q

# For day_since_last_order
def create_relative_datetime_metric_mv(metric_name, num_key):
    q = f"""
        CREATE MATERIALIZED VIEW segment_agg_{metric_name}_mv TO segment_agg AS
        SELECT
            segment_id,
            now() AS time_stamp,
            '{metric_name}' AS metric_name,
            ['max', 'avg', 'med', 'min'] AS `metrics_agg.keys`,
            [toFloat64(max(days)), avg(days), median(days), toFloat64(min(days))] AS `metrics_agg.vals`
        FROM
        (
            SELECT
                segment_id,
                round((toUnixTimestamp(now()) - t)/(24*60*60)) AS days
            FROM segments
            ARRAY JOIN users
            INNER JOIN
            (
                SELECT
                    anonymous_id,
                    pf.num_vals[indexOf(pf.num_keys, '{num_key}')] AS t
                FROM
                (
                    SELECT
                        anonymous_id,
                        argMaxMerge(num_properties.keys) AS num_keys,
                        argMaxMerge(num_properties.vals) AS num_vals
                    FROM user_profile_final
                    GROUP BY
                        tenant_id,
                        anonymous_id
                ) AS pf
            ) AS p ON p.anonymous_id = segments.users
            WHERE t >= 0
        )
        GROUP BY segment_id;
    """
    return q



# Testing...
# print(create_categorical_metrics_mv('gender', 'gender'))
# print(create_numerial_metrics_mv('revenue','total_order'))
#print(create_relative_datetime_metric_mv('day_since_last_order', 'last_order_at'))
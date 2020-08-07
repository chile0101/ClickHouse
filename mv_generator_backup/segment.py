from prile import inject_with


class MigrationSegmentTables(object):
    @inject_with('session', 'session')
    def __init__(self, session):
        self.client = session  # type: sqlalchemy.orm.session.Session

    def create_segments_table(self):
        try:
            self.client.execute("""
                CREATE TABLE segments
                (
                    `segment_id` String,
                    `users` Array(String)
                )
                ENGINE = MergeTree()
                ORDER BY segment_id
            """)
            return True
        except Exception as e:
            raise e

    def drop_segments_table(self):
        try:
            self.client.execute("""DROP TABLE segments""")
            return True
        except Exception as e:
            raise e

    def create_segment_agg_table(self):
        try:
            self.client.execute("""
                CREATE TABLE segment_agg
                (
                    `segment_id` String,
                    `time_stamp` DateTime,
                    `metric_name` String,
                    `metrics_agg` Nested(    keys String,     vals Float32)
                )
                ENGINE = MergeTree()
                PARTITION BY toYYYYMM(time_stamp)
                ORDER BY (segment_id, metric_name)
            """)
            return True
        except Exception as e:
            raise e

    def drop_segment_agg_table(self):
        try:
            self.client.execute("""DROP TABLE segment_agg""")
            return True
        except Exception as e:
            raise e

    def create_segment_agg_final_table(self):
        try:
            self.client.execute("""
                CREATE TABLE segment_agg_final
                (
                    `segment_id` String,
                    `time_stamp_final` SimpleAggregateFunction(max, DateTime),
                    `metric_name` String,
                    `metrics_agg.keys` AggregateFunction(argMax, Array(String), DateTime),
                    `metrics_agg.vals` AggregateFunction(argMax, Array(Float32), DateTime)
                )
                ENGINE = AggregatingMergeTree()
                PARTITION BY tuple()
                ORDER BY (segment_id, metric_name)
            """)
            return True
        except Exception as e:
            raise e

    def drop_segment_agg_final_table(self):
        try:
            self.client.execute("""DROP TABLE segment_agg_final""")
            return True
        except Exception as e:
            raise e

    def create_segment_agg_final_mv(self):
        try:
            self.client.execute("""
                CREATE MATERIALIZED VIEW segment_agg_final_mv TO segment_agg_final AS
                SELECT
                    segment_id,
                    max(time_stamp) AS time_stamp_final,
                    metric_name,
                    argMaxState(metrics_agg.keys, time_stamp) AS `metrics_agg.keys`,
                    argMaxState(metrics_agg.vals, time_stamp) AS `metrics_agg.vals`
                FROM segment_agg
                GROUP BY
                    segment_id,
                    metric_name
            """)
            return True
        except Exception as e:
            raise e

    def drop_segment_agg_final_mv(self):
        try:
            self.client.execute("""DROP TABLE segment_agg_final_mv""")
            return True
        except Exception as e:
            raise e

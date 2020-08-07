from prile import inject_with


class MigrateUserProfileTable(object):
    @inject_with('session', 'session')
    def __init__(self, session):
        self.client = session  # type: sqlalchemy.orm.session.Session

    def create_user_profile_table(self):
        try:
            self.client.execute("""
                CREATE TABLE user_profile
                (
                    `tenant_id` UInt16,
                    `user_id` String,
                    `anonymous_id` String,
                    `identifies` Array(String),
                    `str_properties` Nested(    keys String,     vals String),
                    `num_properties` Nested(    keys String,     vals Float32),
                    `arr_properties` Nested(    keys String,     vals Array(String)),
                    `created_at` DateTime
                )
                ENGINE = MergeTree()
                ORDER BY (tenant_id, anonymous_id, created_at)
            """)
            return True
        except Exception as e:
            raise e

    def drop_user_profile_table(self):
        try:
            self.client.execute("""DROP TABLE user_profile""")
            return True
        except Exception as e:
            raise e

    def create_user_profile_final_table(self):
        try:
            self.client.execute("""
                CREATE TABLE user_profile_final
                (
                    `tenant_id` UInt16,
                    `user_id` AggregateFunction(argMax, String, DateTime),
                    `anonymous_id` String,
                    `identifies` AggregateFunction(argMax, Array(String), DateTime),
                    `str_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
                    `str_properties.vals` AggregateFunction(argMax, Array(String), DateTime),
                    `num_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
                    `num_properties.vals` AggregateFunction(argMax, Array(Float32), DateTime),
                    `arr_properties.keys` AggregateFunction(argMax, Array(String), DateTime),
                    `arr_properties.vals` AggregateFunction(argMax, Array(Array(String)), DateTime),
                    `created_at_final` SimpleAggregateFunction(max, DateTime)
                )
                ENGINE = AggregatingMergeTree()
                ORDER BY (tenant_id, anonymous_id)
            """)
            return True
        except Exception as e:
            raise e

    def drop_user_profile_final_table(self):
        try:
            self.client.execute("""DROP TABLE user_profile_final""")
            return True
        except Exception as e:
            raise e

    def create_user_profile_final_mv(self):
        try:
            self.client.execute("""
                CREATE MATERIALIZED VIEW user_profile_final_mv TO user_profile_final AS
                SELECT
                    tenant_id,
                    argMaxState(user_id, created_at) AS user_id,
                    anonymous_id,
                    argMaxState(identifies, created_at) AS identifies,
                    argMaxState(str_properties.keys, created_at) AS `str_properties.keys`,
                    argMaxState(str_properties.vals, created_at) AS `str_properties.vals`,
                    argMaxState(num_properties.keys, created_at) AS `num_properties.keys`,
                    argMaxState(num_properties.vals, created_at) AS `num_properties.vals`,
                    argMaxState(arr_properties.keys, created_at) AS `arr_properties.keys`,
                    argMaxState(arr_properties.vals, created_at) AS `arr_properties.vals`,
                    max(created_at) AS created_at_final
                FROM user_profile
                GROUP BY
                    tenant_id,
                    anonymous_id            
            """)
            return True
        except Exception as e:
            raise e

    def drop_user_profile_final_mv(self):
        try:
            self.client.execute("""DROP TABLE drop_user_profile_final_mv""")
            return True
        except Exception as e:
            raise e
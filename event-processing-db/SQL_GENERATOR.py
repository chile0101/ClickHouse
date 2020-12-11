def sql_generator(sql: str, params: dict):
    for param_key, param_value in params.items():
        origin_param = f'%({param_key})s'
        if type(param_value) == str:
            param_type_converted = f"'{param_value}'"
        elif type(param_value) == list:
            param_type_converted = f'{param_value}'
        else:
            param_type_converted = str(param_value)

        sql = sql.replace(origin_param, param_type_converted)

    return sql


origin_sql = """
  SELECT 
            id,
            tenant_id,
            anonymous_id,
            toUnixTimestamp(toStartOfDay(at)) AS date_unix,
            toUnixTimestamp(at) AS time_unix,
            session_id,
            event_name,
            arrayZip(`identity.keys`, `identity.vals`) AS identities,
            arrayZip(`str_properties.keys`, `str_properties.vals`) AS str_pros,
            arrayZip(`num_properties.keys`, `num_properties.vals`) AS num_pros,
            arrayZip(`arr_properties.keys`, `arr_properties.vals`) AS arr_pros
        FROM events
        WHERE (tenant_id = %(tenant_id)s) AND (anonymous_id = %(anonymous_id)s) AND event_name NOT IN %(system_events)s 
        ORDER BY at DESC
        LIMIT %(page_number)s * %(page_size)s, %(page_size)s


"""

origin_params = {'tenant_id': 1, 'anonymous_id': 'DF37wf8BWT7auM2Wu6el4070uR8', 'system_events': ['sessionCreated', 'sessionUpdated', 'profileUpdated', 'segmentOptIn', 'segmentOptOut', 'reach_goal', 'goal'], 'page_number': 0, 'page_size': 10}


print(sql_generator(origin_sql, origin_params))

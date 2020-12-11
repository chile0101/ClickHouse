# def sql_generator(sql: str, params: dict):
#     for param_key, param_value in params.items():
#         origin_param = f'%({param_key})s'
#         if type(param_value) == str:
#             param_type_converted = f"'{param_value}'"
#         elif type(param_value) == list:
#             param_type_converted = f'{param_value}'
#         else:
#             param_type_converted = str(param_value)
#
#         sql = sql.replace(origin_param, param_type_converted)
#
#     return sql


sql1 = f"""  
SELECT if(ps.tenant_id != 0, ps.tenant_id, pn.tenant_id) AS tenant_id,
        if(ps.anonymous_id != NULL, ps.anonymous_id, pn.anonymous_id) AS anonymous_id,
        [] AS identities,
        str_pros,
        num_pros,
        [] AS arr_pros,
        ps.at > pn.at ? ps.at : pn.at AS at
FROM
(
    SELECT tenant_id,
           anonymous_id,
           groupArray((str_key, str_val)) AS str_pros,
           max(at) AS at
    FROM profile_str_final_v
    WHERE tenant_id = %(tenant_id)s
        AND str_key IN %(str_pros_filter)s
        AND anonymous_id IN (
                SELECT user
                FROM segment_user_final_v
                WHERE tenant_id = %(tenant_id)s AND segment_id = %(segment_id)s AND status = 1
        )
    GROUP BY tenant_id, anonymous_id
) AS ps
 FULL JOIN
 (
    SELECT tenant_id,
           anonymous_id,
           groupArray((num_key, num_val)) AS num_pros,
           max(at)                        AS at
    FROM profile_num_final_v
    WHERE tenant_id = %(tenant_id)s
      AND num_key IN %(num_pros_filter)s
      AND anonymous_id in (
                SELECT user
                FROM segment_user_final_v
                WHERE tenant_id = %(tenant_id)s AND segment_id = %(segment_id)s AND status = 1
        )
    GROUP BY tenant_id, anonymous_id
) AS pn
ON ps.tenant_id = pn.tenant_id AND ps.anonymous_id = pn.anonymous_id
LIMIT %(limit)s

"""
params1 = {'tenant_id': 1, 'str_pros_filter': ['first_name', 'last_name', 'city', 'country', 'gender'], 'segment_id': 's1', 'num_pros_filter': ['revenue'], 'limit': 9}


print(sql_generator(sql1, params1))
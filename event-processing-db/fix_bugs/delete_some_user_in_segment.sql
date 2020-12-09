show tables;


select * from segment_user;

select * from segment_user where segment_id = '1js1xIzEc03e9XIdm34Iw9u1liz';

alter table segment_user_final delete where segment_id = '1js1xIzEc03e9XIdm34Iw9u1liz'
                                    and user not in ['DE41y1yCW5XyoQihWy8O09CbyzG',
                                                    'DE40gxfXmQaE9A9CF8R9BLan7VC',
                                                    'DE4nnt9BCSY9BMKZ4g9CLvYrN2m',
                                                    'DEzWwZQ07eNN9BynQ0st7Ms19AK',
                                                    'DEzS0pF7yz9Aj1k1ffHCSRSjv2D',
                                                    'DEzRfucrkCOPOl9AnlGrcF5SBqT']
;

select * from segment_user_final_v where segment_id = '1js2dYzHxpzsYDXOrgCVLFQuEJR';

alter table segment_user_final delete where segment_id = '1js2dYzHxpzsYDXOrgCVLFQuEJR'
                                    and user not in ['DE40gxfXmQaE9A9CF8R9BLan7VC','DE40gxfXmQaE9A9CF8R9BLan7VC'];





select * from segment_size  where segment_id = '1js2dYzHxpzsYDXOrgCVLFQuEJR';

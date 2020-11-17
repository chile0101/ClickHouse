select * from events_campaign;

insert into events_campaign values
(randomPrintableASCII(5),1, 'a123', 'conversion','conversion', randomPrintableASCII(3), randomPrintableASCII(3),['utm_campaign','utm_content','utm_source'], ['CAMPAIGN 2.11', 'campaign content', 'campaign source'], [],[],['properties.currency'],['VND'],['properties.total_value'], [120], [],[],'2020-11-02', '2020-11-02 09:35:26');
;

select FROM_UNIXTIME(1604309726);
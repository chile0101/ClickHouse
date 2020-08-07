CREATE TABLE alerts(
  tenant_id     UInt32,
  alert_id      String,
  timestamp     DateTime Codec(Delta, LZ4),
  alert_data    String,
  acked         UInt8 DEFAULT 0,
  ack_time      DateTime DEFAULT toDateTime(0),
  ack_user      LowCardinality(String) DEFAULT ''
)
ENGINE = ReplacingMergeTree(ack_time)
PARTITION BY tuple()
ORDER BY (tenant_id, timestamp, alert_id);


INSERT INTO alerts(tenant_id, alert_id, timestamp, alert_data) values
(1, 1, '2020-01-01 00:00:00', 'data'),
(1, 2, '2020-01-01 00:01:00', 'data'),
(1, 3, '2020-01-01 00:00:00', 'data'),
(1, 4, '2020-01-01 00:01:00', 'data')

INSERT INTO alerts(tenant_id, alert_id, timestamp, alert_data ,acked, ack_user, ack_time)
values
(1, 1, '2020-01-01 00:00:00', 'data', 1, 'chile', now()),
(1, 2, '2020-01-01 00:01:00', 'data', 1, 'chile', now()),

(1, 3, '2020-01-01 00:00:00', 'data', 1, 'chile', now()),
(1, 4, '2020-01-01 00:01:00', 'data', 1, 'chile', now())



INSERT INTO alerts(tenant_id, alert_id, timestamp, alert_data)
SELECT
  toUInt32(rand(1)%1000+1) AS tenant_id,
  randomPrintableASCII(64) as alert_id,
  toDateTime('2020-01-01 00:00:00') + rand(2)%(3600*24*30) as timestamp,
  randomPrintableASCII(1024) as alert_data
FROM numbers(1000000);



INSERT INTO alerts (tenant_id, alert_id, timestamp, alert_data, acked, ack_user, ack_time)
SELECT tenant_id, alert_id, timestamp, alert_data,
  1 as acked,
  concat('user', toString(rand()%1000)) as ack_user,       now() as ack_time
FROM alerts WHERE cityHash64(alert_id) % 99 != 0;




SELECT count() FROM alerts FINAL
WHERE (ack_user = 'chile') AND acked

SELECT count() FROM alerts FINAL
PREWHERE (ack_user = 'chile') AND acked







CREATE TABLE alerts_simple(
  tenant_id     UInt32,
  alert_id      String,
  timestamp     DateTime Codec(Delta, LZ4),
  alert_data    String,
  ack_time DateTime
)
ENGINE = ReplacingMergeTree(ack_time)
PARTITION BY tuple()
ORDER BY (tenant_id, alert_id, timestamp );


INSERT INTO alerts_simple(tenant_id, alert_id, timestamp, alert_data) values
(1, 1, '2020-01-01 00:00:00', randomPrintableASCII(12)),
(1, 2, '2020-01-01 00:00:00', randomPrintableASCII(12)),
(1, 3, '2020-01-01 00:01:00', randomPrintableASCII(12)),
(1, 4, '2020-01-01 00:02:00', randomPrintableASCII(12))

INSERT INTO alerts_simple(tenant_id, alert_id, timestamp, alert_data) values
(1, 1, '2020-01-01 00:12:00', randomPrintableASCII(12)),
(1, 2, '2020-01-01 00:16:00', randomPrintableASCII(12))


alter table alerts_simple add column "ack_time" DateTime Default now()
alter table alerts_simple delete where tenant_id = 1

select tenant_id, alert_id, timestamp, alert_data,ack_time
from alerts_simple

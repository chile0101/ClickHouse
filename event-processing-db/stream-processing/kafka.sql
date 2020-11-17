
-------------------------------------------------------PROFILE UPDATE
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic profile-updated
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic profile-updated --offset 259690 --partition 0

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic profile-updated
./kafka-console-consumer.sh --bootstrap-server "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic profile-updated --offset 259650 --partition 0


------- ----------------------------------------------SEGMENT USER UPDATE
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic segment-user-update
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic segment-user-update --offset 69380 --partition 0

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic segment-user-update
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic segment-user-update --offset 69380 --partition 0



-------------------------------------------------------EVENTS-CAMPAIGN
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic events-campaign
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic events-campaign --offset 10680 --partition 0

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic events-campaign
./kafka-console-consumer.sh --bootstrap-server "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic events-campaign --offset 10680 --partition 0


----------------------------------------------------- REACH GOAL
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic reach_goal
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic reach_goal-events --offset 90535 --partition 0

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic reach_goal
./kafka-console-consumer.sh --bootstrap-server "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic reach_goal --offset 90535 --partition 0


------------------------------------------------------ SESSION UPDATED
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic sessionUpdated
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic sessionUpdated --offset 69380 --partition 0


./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic sessionUpdated
./kafka-console-consumer.sh --bootstrap-server "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic sessionUpdated --offset 69380 --partition 0

----------------------------------------------------- CONVERSION-EVENTS
./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic conversion-events
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic conversion-events --offset 93200 --partition 0

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic conversion-events
./kafka-console-consumer.sh --bootstrap-server "10.110.1.4:9092,10.110.1.4:9093,10.110.1.4:9094" --topic conversion-events --offset 90535 --partition 0


------------ -------------------------------------------EVENTIFY EVENT

./kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic eventify-event
./kafka-console-consumer.sh --bootstrap-server "10.110.1.5:9092,10.110.1.5:9093,10.110.1.5:9094" --topic eventify-event --offset 1253400 --partition 0




----------------------------------------------------------

./kafka-console-consumer.sh --bootstrap-server "localhost:9092" --topic test --from-beginning




show tables;
select * from eventify_stag.user_profile where anonymous_id = '1111'
;

select fromUnixTimestamp64Milli(toInt64(1605322013667));

select * from user_profile where anonymous_id = 'DDqtk7fBzdkADgvdf4MT5KCYvkw';



select * from profile_num where anonymous_id = 'DDz2ygovDHBsZL2x1Mr6keJNTiE';

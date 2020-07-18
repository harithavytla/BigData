CREATE Normal table
---------------------------------------------------------------------------------------------------------------------------------------
create table txns_bucket (txnno int,txndate string,custno int,amount double) row format delimited fields terminated by ',' location '/hive/data/txns_bucketing';

insert into txns_bucket select txnno,txndate,custno,amount from staging;

CREATE table with 3 buckets
-----------------------------------------------------------------------------------------------------------------------------
create table txns_dynamic_bucket (txnno int,txndate string,custno int,amount double) clustered by (txnno) into 3 buckets row format delimited fields terminated by ',' location '/hive/data/txns_dynamic_bucket';

set hive.enforce.bucketing=true;

insert overwrite table txns_dynamic_bucket select * from txns_bucket;

Partitioning with buckets
----------------------------------------
 create table txns_partition_bucketing(txnno int,txndate string,custno int,amount double,product string,city string,state string) partitioned by (country string,spendby string,category string) clustered by (txnno) into 5 buckets row format delimited fields terminated by ',' location '/hive/data/txns_partition_bucketing';

insert overwrite table txns_partition_bucketing partition(country,spendby,category) select txnno,txndate,custno,amount,product,city,state,country,spendby,category from staging;


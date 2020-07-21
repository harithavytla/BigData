HIVE PARTITIONS 
------------------------------------------------------
use hive_data;

Static Partition
-----------------
//based on last column
create external table US_data (txnno INT,txndate STRING,custno INT,amount DOUBLE,category STRING,product STRING,city STRING,state STRING,spendby STRING) PARTITIONED BY (country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' location '/hive/data/txns_partition';

insert into table US_data partition(country='US') select txnno,txndate,custno,amount,category,product,city,state,spendby from staging where country='US';

//based on intermmediary
create external table Dancing_partition (txnno INT,txndate STRING,custno INT,amount DOUBLE,product STRING,city STRING,state STRING,spendby STRING,country STRING) PARTITIONED BY (category STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' location '/hive/data/txns_partition';

insert into table Dancing_partition partition(category='Dancing') select txnno,txndate,custno,amount,product,city,state,spendby,country from staging where category='Dancing';


Dynamic partition 
-------------------

set hive.exec.dynamic.partition.mode=nonstrict	

create external table all_countries (txnno INT,txndate STRING,custno INT,amount DOUBLE,category STRING,product STRING,city STRING,state STRING,spendby STRING) PARTITIONED BY (country STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/hive/data/txns_partition';

insert into table US_data partition(country) select * from staging;





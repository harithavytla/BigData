RDBMS (MySQL)
--------------------------------------------------------
create database hive_data;

use hive_data;

create table txns(txnno INT,txndate VARCHAR(50), custno INT,amount DOUBLE,category VARCHAR(50),product VARCHAR(50),city VARCHAR(50),state VARCHAR(50),spendby VARCHAR(50));

load data local infile '/home/cloudera/hive/data/txns_partition00' into table txns fields terminated by ',';

Import data from RDBMS to HDFS (partition1)
---------------------------------------------------------
sqoop import --connect jdbc:mysql://localhost/hive_data --username root --P --table txns --m 1 --target-dir /hive/data/txns

HIVE
-------------------------------------------------
create database hive_data;

use hive_data;

create table txns(txnno INT,txndate STRING,custno INT,amount DOUBLE,category STRING,product STRING,city STRING,state STRING,spendby STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/hive/data/txns; 

create external table txns_ext(txnno INT,txndate STRING,custno INT,amount DOUBLE,category STRING,product STRING,city STRING,state STRING,spendby STRING) LOCATION '/hive/data/txns_ext';

insert into table txns_ext select * from txns where spendby='cash';

drop table txns;

RDBMS
------------------------------------------------------------
load data local infile '/home/cloudera/hive/data/txns_partition01' into table txns fields terminated by ',';

Import data from RDBMS to HDFS (Incremental)
---------------------------------------------------------
sqoop import --connect jdbc:mysql://localhost/hive_data --username root --P --table txns --m 1 --target-dir /hive/data/txns --incremental append --check-column txnno --last-value 31966

HIVE
-------------------------------------------------
use hive_data;

create table txns(txnno INT,txndate STRING,custno INT,amount DOUBLE,category STRING,product STRING,city STRING,state STRING,spendby STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION '/hive/data/txns; 

load data inpath '/user/hive/warehouse/hive_data.db/txns/part-m-00001' into table txns;

insert into table txns_ext select * from txns where spendby='cash';

drop table txns;




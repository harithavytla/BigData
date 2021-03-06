Create Table in MySQL 
---------------------------------------------
create database project;

use project;

create table part_file_weblog(custid integer(10),
username varchar(30),
quote_count varchar(30),
ip varchar(30),
entry_time varchar(30),
prp_1 varchar(30),
prp_2 varchar(30),
prp_3 varchar(30),
ms varchar(30),
http_type varchar(30),
purchase_category varchar(30),
total_count varchar(30),
purchase_sub_category varchar(30),
http_info varchar(30),
status_code integer(10));


Load data into MySQL Table 
----------------------------------------------------
 load data infile '/home/cloudera/project_1/3.csv' into table part_file_weblog FIELDS terminated by ',' LINES TERMINATED BY '\n';


Import Data from MySQL to HDFS 
---------------------------------------------------
sqoop import --connect jdbc:mysql://localhost/project_1 --username root --P --table part_file_weblog --m 1 --incremental append --check-column custid --last-value 41613 --target-dir /project_1/avro --as-avrodatafile

Create External Table with partitions in HIVE on top of AVSC
-----------------------------------------------------------------
create external table project_1.weblog_ext (custid int,
username string,
quote_count string,
ip string,
prp_1 string,
prp_2 string,
prp_3 string,
ms string,
http_type string,
purchase_category string,
total_count string,
purchase_sub_category string,
http_info string,
status_code int)
partitioned by(year String,month String,date String)
row format SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro location '/project_1/avro' TBLPROPERTIES('avro.schema.url'='/project_1/avsc/part_file_weblog.avsc');

Create Staging Table on top of AVSC
----------------------------------------------------------------
create table staging row format SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED AS avro location '/project_1/avro' TBLPROPERTIES('avro.schema.url'='/project_1/avsc/part_file_weblog.avsc');

Insert records from Staging table to External Table
----------------------------------------------------------------
set hive.exec.dyanamic.partition.mode=nonstrict; 
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert into part_file_weblog partition(year,month,date) select custid,username,quote_count,ip,prp_1,prp_2,prp_3,ms,http_type,purchase_category,
total_count,purchase_sub_category,http_info,status_code,from_unixtime(unix_timestamp(entry_time ,'dd/MMM/yyyy:hh:mm:ss'),'yyyy'),from_unixtime(unix_timestamp(entry_time ,'dd/MMM/yyyy:hh:mm:ss'),'MMM'),from_unixtime(unix_timestamp(entry_time ,'dd/MMM/yyyy:hh:mm:ss'),'dd') from staging where http_info not like ('%Jakarta%');

Drop Staging Table
----------------------------------------------------------------
drop table part_file_weblog_staging ;


sh One_Time_Script.sh daily_weblog_job project_1 root cloudera part_file_weblog custid 0 /project_1/avro /project_1/avsc/part_file_weblog.avsc

sh Execution_Script.sh daily_weblog_job project_1 staging_managed /project_1/avro /project_1/avsc/part_file_weblog.avsc part_file_weblog






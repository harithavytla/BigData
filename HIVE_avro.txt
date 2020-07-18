Create Table (HIVE) on top of AVRO pointing to AVSC
---------------------------------------------------------------
create table customers_avro row format SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro location '/avro/customers' TBLPROPERTIES('avro.schema.url'='/avro/customers.avsc');


Handling Date_Format in Avro (--map-column-java)
---------------------------------------------------------
//Sqoop import
sqoop import --connect jdbc:mysql://localhost/test --username root --password cloudera --table customers --m 1 --map-column-java createdt=String --target-dir /date_format/avro --as-avrodatafile

//Create table with the AVRO and AVSC files
create table customers_avro_avsc row format SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' stored as avro location '/date_format/avro' TBLPROPERTIES('avro.schema.url'='/date_format/avsc/customers.avsc');

//Create Partitioned table 
create table customers_avsc_date_check_part (id int,name string) partitioned by (year string,month string,day string) row format delimited fields terminated by ',' location '/date_format/customers';

//Insert data into partitioned table using the src
insert overwrite table customers_avsc_date_check_part partition(year,month,day) select id,name,substr(created,1,4),substr(created,6,2),substr(created,9,2) from customers_avsc_date_check;


CREATE TABLE hive.default.testtable (
                                        field1 varchar,
                                        field2 integer
)
WITH (
    avro_schema_url = 'hdfs://localhost:9000/presto-schema/avsc/TestTable.avsc',
    external_location = 'hdfs://localhost:9000/tmp/prestoTest/input',
    format = 'AVRO'
)
package com.latticeengines.snowflakedb.exposed.service;

import java.util.List;

import org.apache.avro.Schema;

public interface SnowflakeService {

    void createDatabase(String db, String s3Bucket);

    void dropDatabaseIfExists(String db);

    void createAvroTable(String db, String table, Schema schema, Boolean replace);

    void createAvroTable(String db, String table, Schema schema, Boolean replace, List<String> columnsToExpose);

    void loadAvroTableFromS3(String db, String table, String s3Folder);
}

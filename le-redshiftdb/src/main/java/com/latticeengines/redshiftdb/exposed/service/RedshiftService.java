package com.latticeengines.redshiftdb.exposed.service;

import org.apache.avro.Schema;

public interface RedshiftService {
    void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix);

    void createTable(String tableName, Schema schema);

    void dropTable(String tableName);
}

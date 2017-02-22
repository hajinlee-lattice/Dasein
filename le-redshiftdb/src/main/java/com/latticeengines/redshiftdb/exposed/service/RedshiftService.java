package com.latticeengines.redshiftdb.exposed.service;

import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import org.apache.avro.Schema;

public interface RedshiftService {
    void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix);

    void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema);

    void dropTable(String tableName);
}

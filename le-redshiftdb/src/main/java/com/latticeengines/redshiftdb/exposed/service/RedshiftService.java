package com.latticeengines.redshiftdb.exposed.service;

import java.util.List;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;

public interface RedshiftService {
    void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix);

    void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema);

    void dropTable(String tableName);

    void cloneTable(String srcTable, String tgtTable);

    void createStagingTable(String stageTableName, String targetTableName);

    void updateExistingRowsFromStagingTable(String stageTableName, String targetTableName, String... joinFields);

    void renameTable(String originalTableName, String newTableName);

    void replaceTable(String stageTableName, String targetTableName);

    void analyzeTable(String tableName);

    void vacuumTable(String tableName);

    List<String> getTables(String prefix);
}

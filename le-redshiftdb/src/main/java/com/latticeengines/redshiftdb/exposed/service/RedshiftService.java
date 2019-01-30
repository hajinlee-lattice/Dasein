package com.latticeengines.redshiftdb.exposed.service;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftUnloadParams;

public interface RedshiftService {
    void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix);

    void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema);

    void dropTable(String tableName);

    void cloneTable(String srcTable, String tgtTable);

    void createStagingTable(String stageTableName, String targetTableName);

    void updateExistingRowsFromStagingTable(String stageTableName, String targetTableName, String... joinFields);

    void insertValuesIntoTable(String tableName, List<Pair<String, Class<?>>> schema, List<List<Object>> data);

    void renameTable(String originalTableName, String newTableName);

    void replaceTable(String stageTableName, String targetTableName);

    void analyzeTable(String tableName);

    void vacuumTable(String tableName);

    Long countTable(String tableName);

    boolean hasTable(String tableName);

    List<String> getTables(String prefix);

    void unloadTable(String tableName, String s3bucket, String s3Prefix, RedshiftUnloadParams unloader);
}

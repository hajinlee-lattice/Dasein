package com.latticeengines.prestodb.exposed.service;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;

public interface PrestoDbService {

    boolean tableExists(String tableName);

    void deleteTableIfExists(String tableName);

    default void createTableIfNotExists(String tableName, String dataDir, DataUnit.DataFormat format) {
        createTableIfNotExists(tableName, dataDir, format, null);
    }

    void createTableIfNotExists(String tableName, String dataDir, DataUnit.DataFormat format, List<Pair<String, Class<?>>> partitionKeys);

    List<String> getTablesStartsWith(String tableNamePrefix);

    PrestoDataUnit saveDataUnit(HdfsDataUnit hdfsDataUnit);

}

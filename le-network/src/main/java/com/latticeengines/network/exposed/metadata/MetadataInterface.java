package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Table;

public interface MetadataInterface {

    Boolean resetTables(String customerSpace);

    Boolean createTable(String customerSpace, String tableName, Table table);

    List<String> getImportTableNames(String customerSpace);

    Table getImportTable(String customerSpace, String tableName);

    void deleteImportTable(String customerSpace, String tableName);

    List<String> getTableNames(String customerSpace);

    Table getTable(String customerSpace, String tableName);
}

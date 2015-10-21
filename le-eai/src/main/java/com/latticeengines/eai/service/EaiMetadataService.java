package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;

public interface EaiMetadataService {

    void registerTables(List<Table> tables, ImportContext importContext);

    LastModifiedKey getLastModifiedKey(String customerSpace, Table table);

    List<Table> getTables(String customerSpace);

    void updateTables(String customerSpace, List<Table> tables);

    Table getTable(String customerSpace, String tableName);

    void setLastModifiedTimeStamp(List<Table> tableMetadata, ImportContext importContext);

    void createTable(String customerSpace, Table table);

    void createTables(String customerSpace, List<Table> tables);

}

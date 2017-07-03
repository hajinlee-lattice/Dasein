package com.latticeengines.eai.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;

public interface EaiMetadataService {

    void registerTables(List<Table> tables, ImportContext importContext);

    LastModifiedKey getLastModifiedKey(String customerSpace, Table table);

    void updateTables(String customerSpace, List<Table> tables);

    List<Table> getImportTables(String customerSpace);

    void updateTableSchema(List<Table> tableMetadata, ImportContext importContext);

    Table getTable(String customerSpace, String tableName);

    Map<String, List<Extract>> getExtractsForTable(List<Table> tableMetaData, ImportContext importContext);

}

package com.latticeengines.eai.service;

import java.util.List;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;

public interface EaiMetadataService {

    void registerTables(List<Table> tables, ImportContext importContext);

    LastModifiedKey getLastModifiedKey(String customerSpace, Table table);

    void updateTables(String customerSpace, List<Table> tables);

    List<Table> getImportTables(String customerSpace);

    void updateTableSchema(List<Table> tableMetadata, ImportContext importContext);

}

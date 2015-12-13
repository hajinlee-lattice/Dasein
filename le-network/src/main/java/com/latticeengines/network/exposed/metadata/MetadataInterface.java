package com.latticeengines.network.exposed.metadata;

import com.latticeengines.domain.exposed.metadata.Table;


public interface MetadataInterface {

    Boolean resetTables(String customerSpace);
    
    Boolean createTable(String customerSpace, String tableName, Table table); 
}

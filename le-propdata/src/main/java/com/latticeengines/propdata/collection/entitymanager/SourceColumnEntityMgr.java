package com.latticeengines.propdata.collection.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;
import com.latticeengines.propdata.collection.source.ServingSource;

public interface SourceColumnEntityMgr {

    List<SourceColumn> getSourceColumns(ServingSource source);

    String[] generateCreateTableSqlStatements(ServingSource source, String tableName);

}

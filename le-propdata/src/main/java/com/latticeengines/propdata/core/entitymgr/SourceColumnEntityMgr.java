package com.latticeengines.propdata.core.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.source.ServingSource;

public interface SourceColumnEntityMgr {

    List<SourceColumn> getSourceColumns(ServingSource source);

    String[] generateCreateTableSqlStatements(ServingSource source, String tableName);

}

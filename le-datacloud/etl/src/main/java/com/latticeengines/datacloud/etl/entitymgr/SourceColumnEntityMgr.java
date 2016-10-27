package com.latticeengines.datacloud.etl.entitymgr;

import java.util.List;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

public interface SourceColumnEntityMgr {

    List<SourceColumn> getSourceColumns(String sourceName);

    String[] generateCreateTableSqlStatements(DerivedSource source, String tableName);

}

package com.latticeengines.propdata.engine.common.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.source.DerivedSource;

public interface SourceColumnEntityMgr {

    List<SourceColumn> getSourceColumns(String sourceName);

    String[] generateCreateTableSqlStatements(DerivedSource source, String tableName);

}

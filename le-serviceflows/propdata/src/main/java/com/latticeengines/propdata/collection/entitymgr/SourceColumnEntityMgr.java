package com.latticeengines.propdata.collection.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.source.DerivedSource;

public interface SourceColumnEntityMgr {

    List<SourceColumn> getSourceColumns(DerivedSource source);

    String[] generateCreateTableSqlStatements(DerivedSource source, String tableName);

}

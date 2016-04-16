package com.latticeengines.propdata.engine.common.service;

import com.latticeengines.propdata.core.source.DerivedSource;

public interface SourceColumnService {

    String createTableSql(DerivedSource source, String tableName);

    String createIndicesSql(DerivedSource source, String tableName);

}

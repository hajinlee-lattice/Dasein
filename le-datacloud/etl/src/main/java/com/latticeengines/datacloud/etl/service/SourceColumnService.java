package com.latticeengines.datacloud.etl.service;


import com.latticeengines.datacloud.core.source.DerivedSource;

public interface SourceColumnService {

    String createTableSql(DerivedSource source, String tableName);

    String createIndicesSql(DerivedSource source, String tableName);

}

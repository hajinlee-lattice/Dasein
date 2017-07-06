package com.latticeengines.datacloud.etl.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.service.SourceColumnService;

@Component("sourceColumnService")
public class SourceColumnServiceImpl implements SourceColumnService {

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    @Override
    public String createTableSql(DerivedSource source, String tableName) {
        return StringUtils.join(sourceColumnEntityMgr.generateCreateTableSqlStatements(source, tableName), "\n");
    }

    @Override
    public String createIndicesSql(DerivedSource source, String tableName) {
        String sql = "CREATE INDEX IX_TIMESTAMP ON [" + tableName + "] " + "(["
                + source.getTimestampField() + "])";
        if (source instanceof DomainBased) {
            DomainBased domainBased = (DomainBased) source;
            sql += "CREATE INDEX IX_DOMAIN ON [" + tableName + "] " + "(["
                    + domainBased.getDomainField() + "])";
        }

        return sql;
    }

}

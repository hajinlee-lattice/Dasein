package com.latticeengines.query.factory;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;
import com.latticeengines.query.factory.sqlquery.PrestoSQLQueryFactory;
import com.latticeengines.query.template.PrestoTemplates;
import com.querydsl.sql.Configuration;

@Component("prestoQueryProvider")
public class PrestoQueryProvider extends QueryProvider {
    public static final String PRESTO_USER = "presto";

    @Inject
    private PrestoConnectionService connectionService;

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository, String sqlUser) {
        return PRESTO_USER.equalsIgnoreCase(sqlUser);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser) {
        Configuration configuration = new Configuration(new PrestoTemplates());
        return new PrestoSQLQueryFactory(configuration, connectionService.getPrestoDataSource());
    }

    protected String getCacheKey(AttributeRepository repository, String sqlUser) {
        return sqlUser;
    }

}

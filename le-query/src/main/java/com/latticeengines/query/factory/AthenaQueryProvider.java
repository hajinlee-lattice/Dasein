package com.latticeengines.query.factory;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.AthenaQueryFactory;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;
import com.latticeengines.query.template.AthenaTemplates;
import com.querydsl.sql.Configuration;

@Component("athenaQueryProvider")
public class AthenaQueryProvider extends QueryProvider {
    public static final String ATHENA_USER = "athena";

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository, String sqlUser) {
        return ATHENA_USER.equalsIgnoreCase(sqlUser);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory(AttributeRepository repository, String sqlUser) {
        Configuration configuration = new Configuration(new AthenaTemplates());
        return new AthenaQueryFactory(configuration, null);
    }

    protected String getCacheKey(AttributeRepository repository, String sqlUser) {
        return sqlUser;
    }

}

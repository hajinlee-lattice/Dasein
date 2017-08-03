package com.latticeengines.query.factory;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {

    @Resource(name ="redshiftSegmentDataSource")
    private DataSource redshiftDataSource;

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository) {
        // Redshift provides query against all attribute repository, for now
        return true;
    }

    @Override
    protected SQLQueryFactory getSQLQueryFactory() {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, redshiftDataSource);
    }
}

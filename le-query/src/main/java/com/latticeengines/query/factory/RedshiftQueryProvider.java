package com.latticeengines.query.factory;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {

    @Autowired
    @Qualifier("redshiftDataSource")
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

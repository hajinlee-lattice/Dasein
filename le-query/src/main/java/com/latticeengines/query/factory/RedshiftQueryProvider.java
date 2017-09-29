package com.latticeengines.query.factory;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {

    @PostConstruct
    private void init() {
        jdbcTemplate = BeanFactoryAnnotationUtils.qualifiedBeanOfType(applicationContext, JdbcTemplate.class,
                "redshiftJdbcTemplate");
    }

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository) {
        // Redshift provides query against all attribute repository, for now
        return true;
    }

    @Override
    protected SQLQueryFactory getSQLQueryFactory() {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, jdbcTemplate.getDataSource());
    }
}

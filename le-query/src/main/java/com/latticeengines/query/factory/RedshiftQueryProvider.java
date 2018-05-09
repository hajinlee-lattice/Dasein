package com.latticeengines.query.factory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLQueryFactory;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {
    private static final String USER_SEGMENT = "segment";
    private static final String USER_BATCH = "batch";

    private static final Map<String, DataSource> redshiftDataStores = new HashMap<>();

    @PostConstruct
    private void init() {
        redshiftDataStores.put(
                USER_BATCH, BeanFactoryAnnotationUtils.qualifiedBeanOfType(applicationContext, DataSource.class,
                        "redshiftDataSource")
        );
        redshiftDataStores.put(
                USER_SEGMENT, BeanFactoryAnnotationUtils.qualifiedBeanOfType(applicationContext, DataSource.class,
                        "redshiftSegmentDataSource")
        );
    }

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository) {
        // Redshift provides query against all attribute repository, for now
        return true;
    }

    @Override
    protected SQLQueryFactory getSQLQueryFactory() {
        return getSQLQueryFactory(USER_SEGMENT);
    }

    @Override
    protected SQLQueryFactory getSQLQueryFactory(String sqlUser) {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SQLQueryFactory(configuration, redshiftDataStores.get(sqlUser));
    }
}

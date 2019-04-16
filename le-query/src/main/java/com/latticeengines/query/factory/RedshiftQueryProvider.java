package com.latticeengines.query.factory;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.BeanFactoryAnnotationUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;
import com.latticeengines.query.factory.sqlquery.RedshiftSQLQueryFactory;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;
import com.querydsl.sql.SQLTemplates;

@Component("redshiftQueryProvider")
public class RedshiftQueryProvider extends QueryProvider {
    public static final String USER_SEGMENT = "segment";
    public static final String USER_BATCH = "batch";

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
    public boolean providesQueryAgainst(AttributeRepository repository, String sqlUser) {
        return redshiftDataStores.containsKey(sqlUser);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory() {
        return getSQLQueryFactory(USER_SEGMENT);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory(String sqlUser) {
        SQLTemplates templates = new PostgreSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new RedshiftSQLQueryFactory(configuration, redshiftDataStores.get(sqlUser));
    }

}

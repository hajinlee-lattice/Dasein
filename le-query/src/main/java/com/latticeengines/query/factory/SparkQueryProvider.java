package com.latticeengines.query.factory;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.query.factory.sqlquery.BaseSQLQueryFactory;
import com.latticeengines.query.factory.sqlquery.SparkSQLQueryFactory;
import com.latticeengines.query.template.SparkSQLTemplates;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLTemplates;

@Component("sparkQueryProvider")
public class SparkQueryProvider extends QueryProvider {
    public static final String SPARK_BATCH_USER = "spark-batch";

    @Override
    public boolean providesQueryAgainst(AttributeRepository repository, String sqlUser) {
        return sqlUser.equalsIgnoreCase(SPARK_BATCH_USER);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory() {
        return getSQLQueryFactory(SPARK_BATCH_USER);
    }

    @Override
    protected BaseSQLQueryFactory getSQLQueryFactory(String sqlUser) {
        SQLTemplates templates = new SparkSQLTemplates();
        Configuration configuration = new Configuration(templates);
        return new SparkSQLQueryFactory(configuration, (DataSource)null);
    }

}

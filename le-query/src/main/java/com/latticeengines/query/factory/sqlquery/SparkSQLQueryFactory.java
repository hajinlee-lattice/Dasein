package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;

/**
 * Implementation for SparkSQL 
 *
 */
public class SparkSQLQueryFactory extends BaseSQLQueryFactory {


    public SparkSQLQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    @Override
    public BaseSQLQuery<?> query() {
        return new SparkSQLQuery<Void>(connection, configuration);
    }

}

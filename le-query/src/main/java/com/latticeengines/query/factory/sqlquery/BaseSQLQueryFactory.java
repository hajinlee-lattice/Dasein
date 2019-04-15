package com.latticeengines.query.factory.sqlquery;

import javax.sql.DataSource;

import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLQueryFactory;

/**
 * Base Implementation for QueryFactory 
 *
 */
public abstract class BaseSQLQueryFactory extends SQLQueryFactory {

    public BaseSQLQueryFactory(Configuration configuration, DataSource dataSource) {
        super(configuration, dataSource);
    }

    public abstract BaseSQLQuery<?> query();

}

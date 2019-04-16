package com.latticeengines.query.factory.sqlquery;

import java.sql.Connection;

import javax.inject.Provider;

import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLQuery;

@SuppressWarnings("serial")
public abstract class BaseSQLQuery<T> extends SQLQuery<T> {

    /**
     * Create a new SQLQuery instance
     *
     * @param configuration configuration
     */
    public BaseSQLQuery(Configuration configuration) {
        super((Provider<Connection>) null, configuration);
    }

    /**
     * @param connProvider
     * @param configuration
     */
    public BaseSQLQuery(Provider<Connection> connProvider, Configuration configuration) {
        super(connProvider, configuration);
    }

    public abstract StringTemplate getBitEncodedExpression(StringPath attribute, Integer offset, long bitMask);

    public abstract DateTimeExpressionTemplate getDateTimeTemplate();
}

package com.latticeengines.query.factory.sqlquery;

import java.sql.Connection;

import javax.inject.Provider;

import com.latticeengines.query.template.AthenaTemplates;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.sql.Configuration;

@SuppressWarnings("serial")
public class AthenaQuery<T> extends BaseSQLQuery<T> {

    /**
     * Create a detached SQLQuery instance The query can be attached via the
     * clone method from super class
     */
    public AthenaQuery() {
        super(new Configuration(new AthenaTemplates()));
    }

    /**
     * Create a new SQLQuery instance
     *
     * @param connProvider Connection to use
     * @param configuration configuration
     */
    public AthenaQuery(Provider<Connection> connProvider, Configuration configuration) {
        super(connProvider, configuration);
    }

    @Override
    public StringTemplate getBitEncodedExpression(StringPath attribute, Integer offset, long bitMask) {
        long shiftedMask = bitMask << offset;
        long emptyBits = (long) Math.pow(2, offset);
        return Expressions.stringTemplate("bitwise_and({0}, {1}) / {2}", attribute, shiftedMask, emptyBits);
    }

    @Override
    public DateTimeExpressionTemplate getDateTimeTemplate() {
        throw new UnsupportedOperationException();
    }

}

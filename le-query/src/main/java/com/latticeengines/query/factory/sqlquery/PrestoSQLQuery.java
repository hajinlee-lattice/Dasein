package com.latticeengines.query.factory.sqlquery;

import java.sql.Connection;

import javax.inject.Provider;

import com.latticeengines.query.template.PrestoTemplates;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.WithBuilder;

@SuppressWarnings("serial")
public class PrestoSQLQuery<T> extends BaseSQLQuery<T> {

    /**
     * Create a detached SQLQuery instance The query can be attached via the
     * clone method from super class
     */
    public PrestoSQLQuery() {
        super(new Configuration(new PrestoTemplates()));
    }

    /**
     * Create a new SQLQuery instance
     *
     * @param connProvider Connection to use
     * @param configuration configuration
     */
    public PrestoSQLQuery(Provider<Connection> connProvider, Configuration configuration) {
        super(connProvider, configuration);
    }

    /**
     * SparkSQL does not support columns in With Clause.
     * So, ignoring columns projection and just keeping the alias
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public WithBuilder<SQLQuery<T>> with(Path<?> alias, Path<?>... columns) {
        return new WithBuilder(queryMixin, alias);
    }

    @Override
    public WithBuilder<SQLQuery<T>> withRecursive(Path<?> alias, Path<?>... columns) {
        return super.withRecursive(alias, columns);
    }

    @Override
    public StringTemplate getBitEncodedExpression(StringPath attribute, Integer offset, long bitMask) {
        return Expressions.stringTemplate("bitwise_and(bitwise_logical_shift_right({0}, {1}, 64), {2})", //
                attribute, offset, bitMask);
    }

    @Override
    public DateTimeExpressionTemplate getDateTimeTemplate() {
        throw new UnsupportedOperationException();
    }

}

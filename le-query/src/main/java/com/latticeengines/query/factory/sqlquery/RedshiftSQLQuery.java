package com.latticeengines.query.factory.sqlquery;

import java.sql.Connection;

import javax.inject.Provider;

import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.PostgreSQLTemplates;

@SuppressWarnings("serial")
public class RedshiftSQLQuery<T> extends BaseSQLQuery<T> {

    /**
     * Create a detached SQLQuery instance The query can be attached via the
     * clone method from super class
     */
    public RedshiftSQLQuery() {
        super(new Configuration(new PostgreSQLTemplates()));
    }

    /**
     * Create a new SQLQuery instance
     *
     * @param connProvider Connection to use
     * @param configuration configuration
     */
    public RedshiftSQLQuery(Provider<Connection> connProvider, Configuration configuration) {
        super(connProvider, configuration);
    }

    @Override
    public StringTemplate getBitEncodedExpression(StringPath attribute, Integer offset, long bitMask) {
        return Expressions.stringTemplate("({0}>>{1})&{2}", attribute, offset, bitMask);
    }

    @Override
    public DateTimeExpressionTemplate getDateTimeTemplate() {
        return new RedshiftDateTimeExpressionTemplate();
    }

    private class RedshiftDateTimeExpressionTemplate implements DateTimeExpressionTemplate {

        private static final String CURRENT_DATE = "GETDATE()";

        @Override
        public String getCurrentDate() {
            return CURRENT_DATE;
        }

        @Override
        public String getDateTargetValueOnPeriodTemplate(String datepart, int unit, String timestamp) {
            return String.format("DATEADD('%s', %d, %s)", datepart, unit,
                    getDateOnPeriodTemplate(datepart, timestamp));
        }
    }
}

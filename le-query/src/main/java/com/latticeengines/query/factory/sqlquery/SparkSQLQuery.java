package com.latticeengines.query.factory.sqlquery;

import java.sql.Connection;

import javax.inject.Provider;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.query.template.SparkSQLTemplates;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.core.types.dsl.StringTemplate;
import com.querydsl.sql.Configuration;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.WithBuilder;

@SuppressWarnings("serial")
public class SparkSQLQuery<T> extends BaseSQLQuery<T> {

    /**
     * Create a detached SQLQuery instance The query can be attached via the
     * clone method from super class
     */
    public SparkSQLQuery() {
        super(new Configuration(new SparkSQLTemplates()));
    }

    /**
     * Create a new SQLQuery instance
     *
     * @param connProvider Connection to use
     * @param configuration configuration
     */
    public SparkSQLQuery(Provider<Connection> connProvider, Configuration configuration) {
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
        return Expressions.stringTemplate("shiftRight({0}, {1}) & {2}", attribute, offset, bitMask);
    }

    @Override
    public DateTimeExpressionTemplate getDateTimeTemplate() {
        return new SparkDateTimeExpressionTemplate();
    }

    private class SparkDateTimeExpressionTemplate implements DateTimeExpressionTemplate {

        private static final String ADD_DAYS_FN = "DATE_ADD";
        private static final String ADD_MONTHS_FN = "ADD_MONTHS";
        private static final String CURRENT_DATE = "CURRENT_DATE()";

        @Override
        public String getCurrentDate() {
            return CURRENT_DATE;
        }

        @Override
        public String getDateTargetValueOnPeriodTemplate(String datepart, int unit, String timestamp) {
            String periodStartDate = getDateOnPeriodTemplate(datepart, timestamp);
            PeriodStrategy.Template periodTemplate = PeriodStrategy.Template.fromName(datepart);
            if (periodTemplate == null) {
                throw new UnsupportedOperationException(String.format("Unsupported Period Template for '%s'", datepart));
            }

            String dateAddFunction = null;
            switch (periodTemplate) {
            case Day:
                dateAddFunction = ADD_DAYS_FN;
                break;
            case Week:
                dateAddFunction = ADD_DAYS_FN;
                unit *= 7;
                break;
            case Month:
                dateAddFunction = ADD_MONTHS_FN;
                break;
            case Quarter:
                dateAddFunction = ADD_MONTHS_FN;
                unit *= 3;
                break;
            case Year:
                dateAddFunction = ADD_MONTHS_FN;
                unit *= 12;
                break;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported Period Template '%s'", periodTemplate));
            }

            return String.format("%s(%s, %d)", dateAddFunction, periodStartDate, unit);
        }
    }
}

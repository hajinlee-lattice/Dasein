package com.latticeengines.query.evaluator.impl;

import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.Connective;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("jdbcQueryProcessor")
public class JdbcQueryProcessor extends QueryProcessor {
    @Autowired
    private QueryFactory queryFactory;

    @Override
    public boolean canQuery(DataCollection dataCollection) {
        return dataCollection //
                .getTables() //
                .stream() //
                .allMatch(table -> table.getStorageMechanism() instanceof JdbcStorage);
    }

    @Override
    public SQLQuery<?> process(DataCollection dataCollection, Query query) {
        if (dataCollection.getTables().size() != 1) {
            throw new RuntimeException("Only 1 table is supported at the moment");
        }
        Table table = dataCollection.getTables().stream() //
                .filter(t -> query.getObjectType().toString().equals(t.getInterpretation())) //
                .collect(Collectors.toList()).get(0);
        StringPath tablePath = Expressions.stringPath(table.getName());
        SQLQuery<?> result = queryFactory.getQuery(dataCollection) //
                .from(tablePath) //
                .where(getPredicate(table.getName(), query.getRestriction()));
        processFreeFormRestriction(query.getFreeFormRestriction(), result);
        return result;
    }

    private void processFreeFormRestriction(String freeFormText, SQLQuery<?> query) {

    }

    private BooleanExpression getPredicate(String tableName, Restriction restriction) {
        if (restriction == null) {
            return Expressions.TRUE;
        }

        if (restriction instanceof LogicalRestriction) {
            LogicalRestriction logicalRestriction = (LogicalRestriction) restriction;

            BooleanExpression[] childExpressions = new BooleanExpression[logicalRestriction.getRestrictions().size()];
            childExpressions = logicalRestriction.getRestrictions().stream() //
                    .map(r -> getPredicate(tableName, r)) //
                    .collect(Collectors.toList()) //
                    .toArray(childExpressions);

            if (logicalRestriction.getConnective() == Connective.AND) {
                return Expressions.allOf(childExpressions);
            } else {
                return Expressions.anyOf(childExpressions);
            }
        } else if (restriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) restriction;
            ColumnLookup columnLookup = (ColumnLookup) concreteRestriction.getLhs();
            ValueLookup valueLookup = (ValueLookup) concreteRestriction.getRhs();
            if (columnLookup == null || valueLookup == null) {
                throw new RuntimeException("LHS must be a ColumnLookup and RHS must be a ValueLookup");
            }

            StringPath tablePath = Expressions.stringPath(tableName);
            StringPath columnPath = Expressions.stringPath(tablePath, columnLookup.getColumnName());
            switch (concreteRestriction.getRelation()) {
            case EQUAL:
                return columnPath.eq(valueLookup.getValue().toString());
            case GREATER_OR_EQUAL:
                return columnPath.goe(valueLookup.getValue().toString());
            case GREATER_THAN:
                return columnPath.gt(valueLookup.getValue().toString());
            case LESS_OR_EQUAL:
                return columnPath.loe(valueLookup.getValue().toString());
            case LESS_THAN:
                return columnPath.lt(valueLookup.getValue().toString());
            default:
                throw new RuntimeException(String.format("Unsupported relation %s", concreteRestriction.getRelation()));
            }
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucketRestriction = (BucketRestriction) restriction;
            ColumnLookup columnLookup = bucketRestriction.getLhs();
            int value = bucketRestriction.getValue();

            // TODO implement
            return Expressions.simpleTemplate(Integer.class, String.format("\"%s\"&1", columnLookup.getColumnName()))
                    .eq(1);
        } else {
            throw new RuntimeException(String.format("Unsupported restriction %s", restriction.getClass().getName()));
        }
    }
}

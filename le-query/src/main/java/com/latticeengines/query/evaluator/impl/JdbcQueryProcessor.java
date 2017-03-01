package com.latticeengines.query.evaluator.impl;

import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.object.BusinessObject;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("jdbcQueryProcessor")
public class JdbcQueryProcessor extends QueryProcessor {
    @Autowired
    private QueryFactory queryFactory;

    @Autowired
    private List<BusinessObject> businessObjects;

    @Override
    public SQLQuery<?> process(DataCollection dataCollection, Query query) {
        BusinessObject businessObject = getBusinessObject(query.getObjectType());
        if (businessObject == null) {
            throw new RuntimeException(String.format("Could not locate BusinessObject for ObjectType %s",
                    query.getObjectType()));
        }
        Predicate freeForm = businessObject.processFreeFormSearch(dataCollection, query.getFreeFormTextSearch());
        return businessObject.startQuery(dataCollection, query) //
                .where(freeForm) //
                .where(processRestriction(businessObject.getTable(dataCollection).getName(), query.getRestriction()));
    }

    private BusinessObject getBusinessObject(SchemaInterpretation objectType) {
        return businessObjects.stream().filter(o -> o.getObjectType().equals(objectType)) //
                .findFirst().orElse(null);
    }

    private BooleanExpression processRestriction(String tableName, Restriction restriction) {
        if (restriction == null) {
            return Expressions.TRUE;
        }

        if (restriction instanceof LogicalRestriction) {
            LogicalRestriction logicalRestriction = (LogicalRestriction) restriction;

            BooleanExpression[] childExpressions = new BooleanExpression[logicalRestriction.getRestrictions().size()];
            childExpressions = logicalRestriction.getRestrictions().stream() //
                    .map(r -> processRestriction(tableName, r)) //
                    .collect(Collectors.toList()) //
                    .toArray(childExpressions);

            if (logicalRestriction.getOperator() == LogicalOperator.AND) {
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

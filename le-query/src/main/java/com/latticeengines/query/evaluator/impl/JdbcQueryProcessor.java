package com.latticeengines.query.evaluator.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.object.BusinessObject;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("jdbcQueryProcessor")
public class JdbcQueryProcessor extends QueryProcessor {
    private static final Log log = LogFactory.getLog(JdbcQueryProcessor.class);
    private static final int MAX_PAGE_SIZE = 500;

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
        Predicate freeForm = processFreeFormSearch(dataCollection, query, businessObject);
        SQLQuery<?> sqlQuery = businessObject.startQuery(dataCollection, query).where(freeForm)
                .where(processRestriction(query.getRestriction(), query.getObjectType(), dataCollection))
                .select(getSelect(query, dataCollection));
        sqlQuery = addPaging(sqlQuery, query.getPageFilter());
        log.info(String.format("Generated query:\n%s", sqlQuery.getSQL().getSQL()));
        return sqlQuery;
    }

    private SQLQuery<?> addPaging(SQLQuery<?> sqlQuery, PageFilter pageFilter) {
        if (pageFilter != null) {
            return sqlQuery.limit(Math.min(MAX_PAGE_SIZE, pageFilter.getNumRows())).offset(pageFilter.getRowOffset());
        }
        return sqlQuery;
    }

    private Predicate processFreeFormSearch(DataCollection dataCollection, Query query, BusinessObject businessObject) {
        if (query.getFreeFormTextSearch() != null) {
            return businessObject.processFreeFormSearch(dataCollection, query.getFreeFormTextSearch());
        } else {
            return Expressions.TRUE;
        }
    }

    private Expression<?> getSelect(Query query, DataCollection dataCollection) {
        List<Lookup> lookups = query.getLookups();
        if (lookups == null) {
            Table table = dataCollection.getTable(query.getObjectType());
            List<StringPath> columnPaths = table.getAttributes().stream() //
                    .map((attribute) -> QueryUtils.getColumnPath(table, attribute)) //
                    .collect(Collectors.toList());
            return Expressions.list(columnPaths.toArray(new Expression<?>[columnPaths.size()]));
        } else {
            List<Expression<?>> expressions = new ArrayList<>();
            for (Lookup lookup : lookups) {
                try {
                    expressions.add(resolveLookup(lookup, query.getObjectType(), dataCollection));
                } catch (Exception e) {
                    log.warn(String.format("Could not resolve lookup %s", lookup));
                }
            }
            return Expressions.list(expressions.toArray(new Expression<?>[expressions.size()]));
        }
    }

    private ComparableExpression<String> resolveLookup(Lookup lookup, SchemaInterpretation rootObjectType,
            DataCollection dataCollection) {
        if (lookup instanceof ColumnLookup) {
            ColumnLookup columnLookup = (ColumnLookup) lookup;
            if (columnLookup.getObjectType() == null) {
                columnLookup.setObjectType(rootObjectType);
            }
            Table table = dataCollection.getTable(rootObjectType);
            if (table == null) {
                throw new RuntimeException(String.format("Could not find table of type %s in data collection %s",
                        columnLookup.getObjectType(), dataCollection.getName()));
            }

            Attribute attribute = table.getAttributes().stream()
                    .filter(a -> a.getName() != null && a.getName().equals(columnLookup.getColumnName())) //
                    .findFirst().orElse(null);
            if (attribute == null) {
                throw new RuntimeException(String.format("Could not find attribute with name %s in table %s",
                        columnLookup.getColumnName(), table.getName()));
            }

            return QueryUtils.getColumnPath(table, attribute);

        } else if (lookup instanceof ValueLookup) {
            return Expressions.asComparable(((ValueLookup) lookup).getValue().toString());
        } else {
            throw new RuntimeException(String.format("Unsupported lookup type %s", lookup.getClass().getName()));
        }
    }

    private BusinessObject getBusinessObject(SchemaInterpretation objectType) {
        return businessObjects.stream().filter(o -> o.getObjectType().equals(objectType)) //
                .findFirst().orElse(null);
    }

    private BooleanExpression processRestriction(Restriction restriction, SchemaInterpretation rootObjectType,
            DataCollection dataCollection) {
        if (restriction == null) {
            return Expressions.TRUE;
        }

        if (restriction instanceof LogicalRestriction) {
            LogicalRestriction logicalRestriction = (LogicalRestriction) restriction;

            BooleanExpression[] childExpressions = new BooleanExpression[logicalRestriction.getRestrictions().size()];
            childExpressions = logicalRestriction.getRestrictions().stream() //
                    .map(r -> processRestriction(r, rootObjectType, dataCollection)) //
                    .collect(Collectors.toList()) //
                    .toArray(childExpressions);

            if (logicalRestriction.getOperator() == LogicalOperator.AND) {
                return Expressions.allOf(childExpressions);
            } else {
                return Expressions.anyOf(childExpressions);
            }
        } else if (restriction instanceof ConcreteRestriction) {
            ConcreteRestriction concreteRestriction = (ConcreteRestriction) restriction;
            Lookup lhs = concreteRestriction.getLhs();
            Lookup rhs = concreteRestriction.getRhs();

            ComparableExpression<String> lhsPath = resolveLookup(lhs, rootObjectType, dataCollection);
            ComparableExpression<String> rhsPath = resolveLookup(rhs, rootObjectType, dataCollection);

            switch (concreteRestriction.getRelation()) {
            case EQUAL:
                return lhsPath.eq(rhsPath);
            case GREATER_OR_EQUAL:
                return lhsPath.goe(rhsPath);
            case GREATER_THAN:
                return lhsPath.gt(rhsPath);
            case LESS_OR_EQUAL:
                return lhsPath.loe(rhsPath);
            case LESS_THAN:
                return lhsPath.lt(rhsPath);
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

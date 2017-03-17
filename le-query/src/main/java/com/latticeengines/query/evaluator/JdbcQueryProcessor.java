package com.latticeengines.query.evaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Cardinality;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRelationship;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BucketRange;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.JoinSpecification;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.ObjectUsage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.latticeengines.query.evaluator.lookup.LookupResolverFactory;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.exposed.object.BusinessObject;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.PathBuilder;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("jdbcQueryProcessor")
public class JdbcQueryProcessor extends QueryProcessor {
    private static final Log log = LogFactory.getLog(JdbcQueryProcessor.class);

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

        SQLQuery<?> sqlQuery = startQuery(dataCollection, query) //
                .where(processFreeFormSearch(dataCollection, query, businessObject)) //
                .where(processRestriction(query.getRestriction(), query.getObjectType(), dataCollection)) //
                .select(getSelect(query, dataCollection));

        sqlQuery = addPaging(sqlQuery, query.getPageFilter());

        sqlQuery = addSort(sqlQuery, query.getSort(), query.getObjectType(), dataCollection);

        log.info(String.format("Generated query:\n%s", sqlQuery.getSQL().getSQL()));
        return sqlQuery;
    }

    private SQLQuery<?> startQuery(DataCollection dataCollection, Query query) {
        List<JoinSpecification> joins = query.getNecessaryJoins();
        Table rootTable = dataCollection.getTable(query.getObjectType());

        joins = joins.stream() //
                .filter(js -> js.getDestinationObjectUsage().equals(ObjectUsage.LOOKUP)) //
                .collect(Collectors.toList());

        // FROM TABLE
        SQLQuery<?> sqlQuery = getBusinessObject(query.getObjectType()).startQuery(dataCollection);

        // JOIN T1, T2, T3, etc...
        List<TableRelationship> relationships = getRelationships(joins, rootTable, dataCollection);
        for (TableRelationship relationship : relationships) {
            if (!relationship.getSourceTable().getName().equals(rootTable.getName())) {
                continue;
            }
            if (!relationship.getTargetCardinality().equals(Cardinality.ONE)) {
                continue;
            }

            Table targetTable = dataCollection.getTable(relationship.getTargetTableName());
            PathBuilder<Object> targetTableName = new PathBuilder<>(Object.class, targetTable.getName());

            StringPath sourceColumn = QueryUtils.getColumnPath(rootTable, relationship.getSourceAttributes().get(0));
            StringPath targetColumn = QueryUtils.getColumnPath(targetTable, relationship.getTargetAttributes().get(0));

            sqlQuery = sqlQuery.leftJoin(targetTableName).on(sourceColumn.eq(targetColumn));
        }
        return sqlQuery;
    }

    private List<TableRelationship> getRelationships(List<JoinSpecification> joins, Table rootTable,
            DataCollection dataCollection) {
        List<TableRelationship> sourceRelationships = rootTable.getRelationships();
        // relationships necessary to satisfy all joins
        List<TableRelationship> relationships = new ArrayList<>();
        for (JoinSpecification join : joins) {
            Table destinationTable = dataCollection.getTable(join.getDestinationType());
            if (destinationTable == null) {
                throw new RuntimeException(String.format(
                        "Could not find destination table of type %s in data collection",
                        join.getDestinationObjectUsage()));
            }

            if (join.getDestinationObjectUsage().equals(ObjectUsage.LOOKUP)) {
                TableRelationship relationship = sourceRelationships.stream()
                        .filter(r -> destinationTable.getName().equals(r.getTargetTableName())) //
                        .filter(r -> r.getTargetCardinality().equals(Cardinality.ONE)) //
                        .findFirst() //
                        .orElse(null);
                if (relationship == null) {
                    throw new RuntimeException(String.format(
                            "Cannot find 1-to-1 or many-to-1 relationship to satisfy necessary join %s", join));
                }
                relationships.add(relationship);

            } else if (join.getDestinationObjectUsage().equals(ObjectUsage.EXISTS)) {
                TableRelationship relationship = sourceRelationships.stream()
                        .filter(r -> destinationTable.getName().equals(r.getTargetTableName())) //
                        .filter(r -> r.getSourceCardinality().equals(Cardinality.ONE)) //
                        .filter(r -> r.getTargetCardinality().equals(Cardinality.MANY)) //
                        .findFirst().orElse(null);
                if (relationship == null) {
                    throw new RuntimeException(String.format(
                            "Cannot find 1-to-many relationship to satisfy necessary join %s", join));
                }

                relationships.add(relationship);
            }
        }
        return relationships;
    }

    private SQLQuery<?> addPaging(SQLQuery<?> sqlQuery, PageFilter pageFilter) {
        if (pageFilter != null) {
            return sqlQuery.limit(pageFilter.getNumRows()).offset(pageFilter.getRowOffset());
        }
        return sqlQuery;
    }

    private SQLQuery<?> addSort(SQLQuery<?> sqlQuery, Sort sort, SchemaInterpretation rootObjectType,
            DataCollection dataCollection) {
        if (sort != null) {
            for (Lookup lookup : sort.getLookups()) {
                if (lookup instanceof ColumnLookup) {
                    ComparableExpression<String> resolved = resolveLookup(lookup, rootObjectType, dataCollection)
                            .get(0);
                    if (sort.getDescending()) {
                        sqlQuery = sqlQuery.orderBy(resolved.desc());
                    } else {
                        sqlQuery = sqlQuery.orderBy(resolved.asc());
                    }
                }
            }
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
        if (lookups == null || lookups.size() == 0) {
            return Expressions.constant(1);
        }
        List<Expression<?>> expressions = new ArrayList<>();
        for (Lookup lookup : lookups) {
            try {
                expressions.add(resolveLookup(lookup, query.getObjectType(), dataCollection).get(0));
            } catch (Exception e) {
                log.warn(String.format("Could not resolve lookup %s", lookup));
            }
        }
        return Expressions.list(expressions.toArray(new Expression<?>[expressions.size()]));
    }

    private List<ComparableExpression<String>> resolveLookup(Lookup lookup, SchemaInterpretation rootObjectType,
            DataCollection dataCollection, Lookup optionalLhsLookup) {
        LookupResolverFactory factory = new LookupResolverFactory(new LookupResolverFactory.Builder() //
                .lookup(lookup) //
                .rootObjectType(rootObjectType) //
                .dataCollection(dataCollection) //
                .secondaryLookup(optionalLhsLookup));
        LookupResolver resolver = factory.getLookupResolver();
        return resolver.resolve();
    }

    private List<ComparableExpression<String>> resolveLookup(Lookup lookup, SchemaInterpretation rootObjectType,
            DataCollection dataCollection) {
        return resolveLookup(lookup, rootObjectType, dataCollection, null);
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

            ComparableExpression<String> lhsPath = resolveLookup(lhs, rootObjectType, dataCollection).get(0);
            List<ComparableExpression<String>> rhsPaths = resolveLookup(rhs, rootObjectType, dataCollection, lhs);

            switch (concreteRestriction.getRelation()) {
            case EQUAL:
                return lhsPath.eq(rhsPaths.get(0));
            case GREATER_OR_EQUAL:
                return lhsPath.goe(rhsPaths.get(0));
            case GREATER_THAN:
                return lhsPath.gt(rhsPaths.get(0));
            case LESS_OR_EQUAL:
                return lhsPath.loe(rhsPaths.get(0));
            case LESS_THAN:
                return lhsPath.lt(rhsPaths.get(0));
            case IN_RANGE:
                return lhsPath.between(rhsPaths.get(0), rhsPaths.get(1));
            default:
                throw new RuntimeException(String.format("Unsupported relation %s", concreteRestriction.getRelation()));
            }
        } else if (restriction instanceof BucketRestriction) {
            BucketRestriction bucketRestriction = (BucketRestriction) restriction;
            BucketRange range = bucketRestriction.getBucket();
            ConcreteRestriction placeholder;
            if (range.getMin() != null && range.getMax() != null && range.getMin().equals(range.getMax())) {
                placeholder = new ConcreteRestriction(false, bucketRestriction.getLhs(), ComparisonType.EQUAL, //
                        new ValueLookup(bucketRestriction.getBucket().getMin()));
            } else {
                placeholder = new ConcreteRestriction(false, bucketRestriction.getLhs(), ComparisonType.IN_RANGE, //
                        new RangeLookup(bucketRestriction.getBucket().getMin(), bucketRestriction.getBucket().getMax()));
            }
            return processRestriction(placeholder, rootObjectType, dataCollection);
        } else if (restriction instanceof ExistsRestriction) {
            ExistsRestriction existsRestriction = (ExistsRestriction) restriction;

            Table parent = dataCollection.getTable(rootObjectType);
            TableRelationship relationship = parent
                    .getRelationships()
                    .stream()
                    .filter(r -> r.getSourceCardinality() == Cardinality.ONE
                            && r.getTargetCardinality() == Cardinality.MANY //
                            && r.getTargetTableName() != null)
                    .filter(r -> r.getTargetTableName().equals(
                            dataCollection.getTable(existsRestriction.getObjectType()).getName())) //
                    .findFirst() //
                    .orElse(null);

            if (relationship == null) {
                throw new RuntimeException(
                        String.format(
                                "Could not find a one-to-many relationship from table %s to table of type %s to process exists restriction %s",
                                parent.getName(), existsRestriction.getObjectType(), existsRestriction));
            }
            Table child = dataCollection.getTable(existsRestriction.getObjectType());

            StringPath parentJoinColumn = QueryUtils.getColumnPath(parent, relationship.getSourceAttributes().get(0));
            StringPath childJoinColumn = QueryUtils.getColumnPath(child, relationship.getTargetAttributes().get(0));

            Restriction innerRestriction = existsRestriction.getRestriction();
            SQLQuery<?> query = queryFactory.getQuery(dataCollection).from(QueryUtils.getTablePath(child));
            BooleanExpression innerPredicate = processRestriction(innerRestriction, existsRestriction.getObjectType(),
                    dataCollection);
            query = query.where(innerPredicate.and(parentJoinColumn.eq(childJoinColumn)));
            if (existsRestriction.getNegate()) {
                return query.notExists();
            } else {
                return query.exists();
            }
        } else {
            throw new RuntimeException(String.format("Unsupported restriction %s", restriction.getClass().getName()));
        }
    }
}

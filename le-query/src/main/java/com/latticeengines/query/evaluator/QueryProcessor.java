package com.latticeengines.query.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.graph.traversal.impl.BreadthFirstSearch;
import com.latticeengines.common.exposed.visitor.Visitor;
import com.latticeengines.common.exposed.visitor.VisitorContext;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.GroupBy;
import com.latticeengines.domain.exposed.query.JoinSpecification;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.query.SubQuery;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.query.evaluator.lookup.LookupResolver;
import com.latticeengines.query.evaluator.lookup.LookupResolverFactory;
import com.latticeengines.query.evaluator.restriction.RestrictionResolver;
import com.latticeengines.query.evaluator.restriction.RestrictionResolverFactory;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.latticeengines.query.util.AttrRepoUtils;
import com.latticeengines.query.util.QueryUtils;
import com.querydsl.core.types.EntityPath;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.SubQueryExpression;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("queryProcessor")
public class QueryProcessor {

    @Inject
    private QueryFactory queryFactory;

    public QueryFactory getQueryFactory() {
        return queryFactory;
    }

    public SQLQuery<?> process(AttributeRepository repository, Query query, String sqlUser) {
        query.analyze();
        LookupResolverFactory resolverFactory = new LookupResolverFactory(repository, this, sqlUser);
        RestrictionResolverFactory rrFactory = new RestrictionResolverFactory(resolverFactory, query.getExistsJoins(),
                queryFactory, this);
        resolverFactory.setRestrictionResolverFactory(rrFactory);

        SQLQuery<?> sqlQuery = from(repository, query, sqlUser);

        if (query.getRestriction() != null) {
            BooleanExpression whereClause = processRestriction(query.getRestriction(), resolverFactory,
                    query.getExistsJoins());
            sqlQuery = sqlQuery.where(whereClause);
        }
        if (StringUtils.isNotBlank(query.getFreeFormTextSearch())
                && !query.getFreeFormTextSearchAttributes().isEmpty()) {
            sqlQuery = sqlQuery.where(processFreeTextSearch(query));
        }

        if (query.getDistinct() != null && query.getDistinct()) {
            sqlQuery = sqlQuery.distinct();
        }

        sqlQuery = sqlQuery.select(getSelect(resolverFactory, query.getLookups(), //
                Boolean.TRUE.equals(query.getDistinct())));
        if (query.getPageFilter() != null && !query.containEntityForExists()) {
            sqlQuery = addPaging(sqlQuery, query.getPageFilter());
        }
        if (query.getSort() != null) {
            sqlQuery = addSort(sqlQuery, query.getSort(), resolverFactory);
        }

        if (query.getGroupBy() != null) {
            sqlQuery = addGroupBy(sqlQuery, query.getGroupBy(), resolverFactory);
        }

        return sqlQuery;
    }

    /**
     * FROM TABLE or FROM (sub query) AS alias
     */
    private SQLQuery<?> from(AttributeRepository repository, Query query, String sqlUser) {
        SubQuery subQuery = query.getSubQuery();
        SQLQuery<?> sqlQuery = null;

        if (query.hasPreprocessed()) {
            sqlQuery = (SQLQuery<?>) query.getSubQuery().getSubQueryExpression();
        } else {
            sqlQuery = queryFactory.getQuery(repository, sqlUser);
        }

        for (SubQuery sq : query.getCommonTableQueryList()) {
            StringPath aliasTable = AttrRepoUtils.getTablePath(sq.getAlias());
            StringPath[] projectedColumns = sq.getProjections().stream().map(QueryUtils::getAttributePath)
                    .toArray(StringPath[]::new);
            if (projectedColumns.length != 0) {
                sqlQuery = sqlQuery.with(aliasTable, projectedColumns)
                        .as(processSubueryExpression(repository, sq, sqlUser, false));
            } else {
                sqlQuery = sqlQuery.with(aliasTable, processSubueryExpression(repository, sq, sqlUser, false));
            }
        }
        if (query.hasPreprocessed()) {
            return sqlQuery;
        }
        if (subQuery != null) {
            Expression<?> subQueryExpression = processSubueryExpression(repository, subQuery, sqlUser, true);
            sqlQuery = sqlQuery.from(subQueryExpression);
        } else {
            BusinessEntity mainEntity = query.getMainEntity();
            StringPath mainTable = AttrRepoUtils.getTablePath(repository, mainEntity);
            sqlQuery = sqlQuery.from(mainTable.as(mainEntity.name()));
        }
        return addJoins(sqlQuery, repository, query);
    }

    private Expression<?> processSubueryExpression(AttributeRepository repository, SubQuery subQuery, String sqlUser,
            boolean setAlias) {
        if (subQuery.getSubQueryExpression() != null) {
            return (SubQueryExpression<?>) subQuery.getSubQueryExpression();
        } else {
            return (setAlias) ? process(repository, subQuery.getQuery(), sqlUser).as(subQuery.getAlias())
                    : process(repository, subQuery.getQuery(), sqlUser);
        }
    }

    /**
     * JOIN T1, T2, T3, etc...
     */
    private SQLQuery<?> addJoins(SQLQuery<?> sqlQuery, AttributeRepository repository, Query query) {
        sqlQuery = addLookupJoins(sqlQuery, repository, query);
        return sqlQuery;
    }

    private Set<BusinessEntity> checkAndGetClusteredEntities(BusinessEntity mainEntity,
            List<JoinSpecification> lookupJoins) {
        Set<BusinessEntity> clusteredEntities = new HashSet<>();

        if (mainEntity != null) {
            clusteredEntities.add(mainEntity);
        }

        for (JoinSpecification join : lookupJoins) {
            BusinessEntity target = join.getDestinationEntity();
            // from all seen entities find one can join the current target
            clusteredEntities.stream() //
                    .map(e -> e.join(target)) //
                    .filter(Objects::nonNull) //
                    .findAny() //
                    .orElseThrow(() -> new QueryEvaluationException(
                            "Broken Connectivity: Cannot find a connected path from entity " + join.getSourceEntity()
                                    + " to entity " + target + "."));

            clusteredEntities.add(target);
        }

        return clusteredEntities;
    }

    private static class JoinedEntityVisitor implements Visitor {
        private SQLQuery<?> sqlQuery;
        private BusinessEntity mainEntity;
        private Set<BusinessEntity> joinedEntities;
        private AttributeRepository repository;

        JoinedEntityVisitor(SQLQuery<?> sqlQuery, BusinessEntity mainEntity, Set<BusinessEntity> joinedEntities,
                AttributeRepository repository) {
            this.sqlQuery = sqlQuery;
            this.mainEntity = mainEntity;
            this.joinedEntities = joinedEntities;
            this.repository = repository;
        }

        @Override
        public void visit(Object object, VisitorContext ctx) {
            BusinessEntity entity = (BusinessEntity) object;
            if (object != mainEntity && joinedEntities.contains(entity)) {
                BusinessEntity parent = (BusinessEntity) ctx.getProperty("parent");
                BusinessEntity.Relationship relationship = parent.join(entity);
                BusinessEntity.Cardinality cardinality = relationship.getCardinality();
                // JOIN T1
                EntityPath<String> targetTableName = AttrRepoUtils.getTablePathBuilder(repository, entity);
                switch (cardinality) {
                case ONE_TO_MANY:
                case ONE_TO_ONE:
                    sqlQuery = sqlQuery.leftJoin(targetTableName, Expressions.stringPath(entity.name()));
                    break;
                default:
                    sqlQuery = sqlQuery.join(targetTableName, Expressions.stringPath(entity.name()));
                }
                for (Predicate predicate : QueryUtils.getJoinPredicates(relationship)) {
                    sqlQuery = sqlQuery.on(predicate);
                }
            }
        }
    }

    private SQLQuery<?> addLookupJoins(SQLQuery<?> sqlQuery, AttributeRepository repository, Query query) {
        // first, we make sure all joined entities are in a cluster and
        // reachable, so we can do a valid join
        Set<BusinessEntity> joinedEntities = checkAndGetClusteredEntities(query.getMainEntity(),
                query.getLookupJoins());
        // generate join in bfs order, so relationships (join keys) appear only
        // after the entities (tables) are defined
        if (!joinedEntities.isEmpty()) {
            BreadthFirstSearch bfs = new BreadthFirstSearch();
            bfs.run(query.getMainEntity(),
                    new JoinedEntityVisitor(sqlQuery, query.getMainEntity(), joinedEntities, repository));
        }
        return sqlQuery;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Expression<?> getSelect(LookupResolverFactory factory, List<Lookup> lookups, boolean distinct) {
        List<Expression<?>> expressions = new ArrayList<>();
        for (Lookup lookup : lookups) {
            LookupResolver resolver = factory.getLookupResolver(lookup.getClass());
            Expression<?> expression = resolver.resolveForSelect(lookup, !distinct);
            expressions.add(expression);
        }
        if (expressions.size() == 0) {
            return Expressions.constant(1);
        }
        return Expressions.list(expressions.toArray(new Expression<?>[0]));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private BooleanExpression processRestriction(Restriction restriction, LookupResolverFactory resolverFactory,
            List<JoinSpecification> existsJoins) {
        RestrictionResolverFactory factory = new RestrictionResolverFactory(resolverFactory, existsJoins, queryFactory,
                this);
        RestrictionResolver resolver = factory.getRestrictionResolver(restriction.getClass());
        return resolver.resolve(restriction);
    }

    private SQLQuery<?> addPaging(SQLQuery<?> sqlQuery, PageFilter pageFilter) {
        if (pageFilter.getNumRows() > 0) {
            sqlQuery = sqlQuery.limit(pageFilter.getNumRows());
        }
        if (pageFilter.getRowOffset() > 0) {
            sqlQuery = sqlQuery.offset(pageFilter.getRowOffset());
        }
        return sqlQuery;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private SQLQuery<?> addSort(SQLQuery<?> sqlQuery, Sort sort, LookupResolverFactory resolverFactory) {
        if (sort != null) {
            for (Lookup lookup : sort.getLookups()) {
                LookupResolver resolver = resolverFactory.getLookupResolver(lookup.getClass());
                if (lookup instanceof AttributeLookup || lookup instanceof SubQueryAttrLookup) {
                    ComparableExpression<String> resolved = Expressions
                            .asComparable(resolver.resolveForSelect(lookup, false));
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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private SQLQuery<?> addGroupBy(SQLQuery<?> sqlQuery, GroupBy groupBy, LookupResolverFactory resolverFactory) {
        if (groupBy != null) {
            for (Lookup lookup : groupBy.getLookups()) {
                LookupResolver resolver = resolverFactory.getLookupResolver(lookup.getClass());
                ComparableExpression<String> resolved = Expressions
                        .asComparable(resolver.resolveForSelect(lookup, false));
                sqlQuery = sqlQuery.groupBy(resolved);
            }
            if (groupBy.getHaving() != null) {
                Restriction restriction = groupBy.getHaving();
                BooleanExpression booleanExpression = processRestriction(restriction, resolverFactory,
                        Collections.emptyList());
                sqlQuery = sqlQuery.having(booleanExpression);
            }
        }
        return sqlQuery;
    }

    private BooleanExpression processFreeTextSearch(Query query) {
        BooleanExpression[] expressions = new BooleanExpression[query.getFreeFormTextSearchAttributes().size()];

        // Just check whether each attribute LIKE '%freeFormRestriction%'
        // These attributes are expected to exist so no metadata check required
        expressions = query.getFreeFormTextSearchAttributes().stream() //
                .map(attr -> {
                    StringPath columnPath = QueryUtils.getAttributePath(attr.getEntity(), attr.getAttribute());
                    return columnPath.containsIgnoreCase(query.getFreeFormTextSearch());
                }) //
                .collect(Collectors.toList()) //
                .toArray(expressions);

        return Expressions.anyOf(expressions);
    }
}

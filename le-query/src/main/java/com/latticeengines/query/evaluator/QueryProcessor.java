package com.latticeengines.query.evaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.ComparableExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.core.types.dsl.StringPath;
import com.querydsl.sql.SQLQuery;

@Component("queryProcessor")
public class QueryProcessor {

    @Autowired
    private QueryFactory queryFactory;

    public SQLQuery<?> process(AttributeRepository repository, Query query) {
        query.analyze();

        LookupResolverFactory resolverFactory = new LookupResolverFactory(repository);
        RestrictionResolverFactory rrFactory = new RestrictionResolverFactory(resolverFactory, query.getExistsJoins(),
                queryFactory);
        resolverFactory.setRestrictionResolverFactory(rrFactory);

        SQLQuery<?> sqlQuery = from(repository, query);
        if (query.getRestriction() != null) {
            BooleanExpression whereClause = processRestriction(query.getRestriction(), resolverFactory,
                    query.getExistsJoins());
            sqlQuery = sqlQuery.where(whereClause);
        }
        if (StringUtils.isNotBlank(query.getFreeFormTextSearch())
                && !query.getFreeFormTextSearchAttributes().isEmpty()) {
            sqlQuery = sqlQuery.where(processFreeTextSearch(query));
        }
        sqlQuery = sqlQuery.select(getSelect(resolverFactory, query.getLookups()));
        if (query.getPageFilter() != null) {
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
    private SQLQuery<?> from(AttributeRepository repository, Query query) {
        SubQuery subQuery = query.getSubQuery();
        SQLQuery<?> sqlQuery;
        if (subQuery != null) {
            Expression<?> subQueryExpression = process(repository, subQuery.getQuery()).as(subQuery.getAlias());
            sqlQuery = queryFactory.getQuery(repository).from(subQueryExpression);
        } else {
            BusinessEntity mainEntity = query.getMainEntity();
            StringPath mainTable = AttrRepoUtils.getTablePath(repository, mainEntity);
            sqlQuery = queryFactory.getQuery(repository).from(mainTable.as(mainEntity.name()));
        }
        return addJoins(sqlQuery, repository, query);
    }

    /**
     * JOIN T1, T2, T3, etc...
     */
    private SQLQuery<?> addJoins(SQLQuery<?> sqlQuery, AttributeRepository repository, Query query) {
        List<BusinessEntity> joinedEntities = new ArrayList<>();
        joinedEntities.add(query.getMainEntity());
        List<JoinSpecification> lookupJoins = query.getLookupJoins();
        List<Predicate> joinKeys = new ArrayList<>();
        for (JoinSpecification join : lookupJoins) {
            BusinessEntity target = join.getDestinationEntity();
            // from all seen entities find one can join the current target
            BusinessEntity.Relationship relationship = joinedEntities.stream() //
                    .map(e -> e.join(target)) //
                    .filter(Objects::nonNull) //
                    .findAny().orElse(null);
            if (relationship == null) {
                throw new QueryEvaluationException(
                        "Broken Connectivity: Cannot find a connected path from entity " + join.getSourceEntity() + " to entity " + target + ".");
            }
            // JOIN T1
            EntityPath<String> targetTableName = AttrRepoUtils.getTablePathBuilder(repository, target);
            sqlQuery = sqlQuery.leftJoin(targetTableName, Expressions.stringPath(target.name()));
            joinKeys.addAll(QueryUtils.getJoinPredicates(relationship));
            joinedEntities.add(target);
        }
        for (Predicate predicate : joinKeys) {
            sqlQuery = sqlQuery.on(predicate);
        }
        return sqlQuery;
    }

    @SuppressWarnings("unchecked")
    private Expression<?> getSelect(LookupResolverFactory factory, List<Lookup> lookups) {
        List<Expression<?>> expressions = new ArrayList<>();
        for (Lookup lookup : lookups) {
            LookupResolver resolver = factory.getLookupResolver(lookup.getClass());
            Expression<?> expression = resolver.resolveForSelect(lookup, true);
            expressions.add(expression);
        }
        if (expressions.size() == 0) {
            return Expressions.constant(1);
        }
        return Expressions.list(expressions.toArray(new Expression<?>[expressions.size()]));
    }

    @SuppressWarnings("unchecked")
    private BooleanExpression processRestriction(Restriction restriction, LookupResolverFactory resolverFactory,
            List<JoinSpecification> existsJoins) {
        RestrictionResolverFactory factory = new RestrictionResolverFactory(resolverFactory, existsJoins,
                queryFactory);
        RestrictionResolver resolver = factory.getRestrictionResolver(restriction.getClass());
        return resolver.resolve(restriction);
    }

    private SQLQuery<?> addPaging(SQLQuery<?> sqlQuery, PageFilter pageFilter) {
        return sqlQuery.limit(pageFilter.getNumRows()).offset(pageFilter.getRowOffset());
    }

    @SuppressWarnings("unchecked")
    private SQLQuery<?> addSort(SQLQuery<?> sqlQuery, Sort sort, LookupResolverFactory resolverFactory) {
        if (sort != null) {
            LookupResolver resolver = resolverFactory.getLookupResolver(AttributeLookup.class);
            for (Lookup lookup : sort.getLookups()) {
                if (lookup instanceof AttributeLookup) {
                    ComparableExpression<String> resolved = Expressions.asComparable(resolver.resolveForSelect(lookup, false));
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

    @SuppressWarnings("unchecked")
    private SQLQuery<?> addGroupBy(SQLQuery<?> sqlQuery, GroupBy groupBy, LookupResolverFactory resolverFactory) {
        if (groupBy != null) {
            for (Lookup lookup: groupBy.getLookups()) {
                LookupResolver resolver = resolverFactory.getLookupResolver(lookup.getClass());
                ComparableExpression<String> resolved = Expressions.asComparable(resolver.resolveForSelect(lookup, false));
                sqlQuery = sqlQuery.groupBy(resolved);
            }
            if (groupBy.getHaving() != null) {
                Restriction restriction = groupBy.getHaving();
                BooleanExpression booleanExpression = processRestriction(restriction, resolverFactory, Collections.emptyList());
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
                    return columnPath.toUpperCase().contains(query.getFreeFormTextSearch().toUpperCase());
                }) //
                .collect(Collectors.toList()) //
                .toArray(expressions);

        return Expressions.anyOf(expressions);
    }
}

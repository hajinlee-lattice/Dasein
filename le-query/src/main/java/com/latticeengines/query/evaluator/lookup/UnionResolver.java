package com.latticeengines.query.evaluator.lookup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.graph.traversal.impl.DepthFirstSearch;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.LogicalOperator;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.UnionLookup;
import com.latticeengines.query.exposed.factory.QueryFactory;
import com.querydsl.core.types.SubQueryExpression;
import com.querydsl.sql.SQLExpressions;
import com.querydsl.sql.SQLQuery;
import com.querydsl.sql.Union;

public class UnionResolver extends BaseLookupResolver<UnionLookup> implements LookupResolver<UnionLookup> {
    private LookupResolverFactory resolverFactory;

    public UnionResolver(AttributeRepository repository, LookupResolverFactory factory) {
        super(repository);
        this.resolverFactory = factory;
    }

    private static class UnionCollector {
        private List<Union> unionList = new ArrayList<>();

        UnionCollector withUnion(Union... unions) {
            unionList.addAll(Arrays.asList(unions));
            return this;
        }

        int size() {
            return unionList.size();
        }

        void reset(Union union) {
            this.unionList.clear();
            this.unionList.add(union);
        }

        List<Union> asList() {
            return this.unionList;
        }

        Union[] asArray() {
            return unionList.toArray(new Union[unionList.size()]);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Union<?> resolveForUnion(UnionLookup lookup) {
        Map<LogicalRestriction, List<Lookup>> lookupMap = lookup.getLookupMap();
        Map<LogicalRestriction, UnionCollector> unionMap = new HashMap<>();

        // process leaf nodes
        for (LogicalRestriction logicalRestriction : lookupMap.keySet()) {
            List<Lookup> childLookups = lookupMap.get(logicalRestriction);
            if (childLookups != null && childLookups.size() != 0) {
                SQLQuery[] childQueries = childLookups.stream().map(x -> {
                    LookupResolver resolver = resolverFactory.getLookupResolver(x.getClass());
                    return resolver.resolveForFrom(x);
                }).toArray(SQLQuery[]::new);

                Union<?> union = mergeQueryResult(logicalRestriction.getOperator(), childQueries);
                UnionCollector collector = unionMap.getOrDefault(logicalRestriction, new UnionCollector());
                unionMap.put(logicalRestriction, collector.withUnion(union));
            }
        }

        DepthFirstSearch dfs = new DepthFirstSearch();
        dfs.run(lookup.getRootRestriction(), (object, ctx) -> {
            if (object instanceof LogicalRestriction) {
                LogicalRestriction logicalRestriction = (LogicalRestriction) object;
                UnionCollector collector = unionMap.get(logicalRestriction);

                if (collector == null || collector.size() == 0) {
                    // ignore or log error since this shouldn't happen
                    return;
                }

                Union<?> merged;
                if (collector.size() > 1) {
                    merged = mergeQueryResult(logicalRestriction.getOperator(), collector.asArray());
                    collector.reset(merged);
                } else {
                    merged = collector.asList().get(0);
                }

                LogicalRestriction parent = (LogicalRestriction) ctx.getProperty("parent");
                if (parent != null) {
                    UnionCollector parentCollector = unionMap.getOrDefault(logicalRestriction, new UnionCollector());
                    parentCollector.withUnion(merged);
                    unionMap.put(parent, parentCollector);
                }
            }
        }, false);

        return unionMap.get(lookup.getRootRestriction()).asList().get(0);
    }

    @SuppressWarnings({ "unchecked", "rawtype" })
    private Union<?> mergeQueryResult(LogicalOperator ops, SubQueryExpression[] childQueries) {
        return (LogicalOperator.AND == ops) //
                ? SQLExpressions.intersect(childQueries)
                : SQLExpressions.union(childQueries);
    }

}

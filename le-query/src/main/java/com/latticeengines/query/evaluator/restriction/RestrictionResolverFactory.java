package com.latticeengines.query.evaluator.restriction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.ExistsRestriction;
import com.latticeengines.domain.exposed.query.JoinSpecification;
import com.latticeengines.domain.exposed.query.LogicalRestriction;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.evaluator.lookup.LookupResolverFactory;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.exposed.factory.QueryFactory;

public final class RestrictionResolverFactory {

    private LookupResolverFactory lookupFactory;
    private List<JoinSpecification> existsJoins;
    private QueryFactory queryFactory;
    private QueryProcessor queryProcessor;
    private Map<String, RestrictionResolver> resolvers = new HashMap<>();

    public RestrictionResolverFactory(LookupResolverFactory lookupFactory,
                                      List<JoinSpecification> existsJoins,
                                      QueryFactory queryFactory,
                                      QueryProcessor queryProcessor) {
        this.lookupFactory = lookupFactory;
        this.existsJoins = existsJoins;
        this.queryFactory = queryFactory;
        this.queryProcessor = queryProcessor;
    }

    @SuppressWarnings("unchecked")
    public <T extends Restriction> RestrictionResolver<T> getRestrictionResolver(Class<T> restrictionType) {
        if (!resolvers.containsKey(restrictionType.getSimpleName())) {
            initializeResolver(restrictionType);
        }
        return (RestrictionResolver<T>) resolvers.get(restrictionType.getSimpleName());
    }

    private <T extends Restriction> void initializeResolver(Class<T> restrictionType) {
        if (restrictionType.isAssignableFrom(ConcreteRestriction.class)) {
            resolvers.put(restrictionType.getSimpleName(), new ConcreteResolver(this));
            return;
        }
        if (restrictionType.isAssignableFrom(ExistsRestriction.class)) {
            resolvers.put(restrictionType.getSimpleName(), new ExistsResolver(this));
            return;
        }
        if (restrictionType.isAssignableFrom(LogicalRestriction.class)) {
            resolvers.put(restrictionType.getSimpleName(), new LogicalResolver(this));
            return;
        }
        throw new QueryEvaluationException("Do not support restriction of type " + restrictionType + " yet.");
    }

    LookupResolverFactory getLookupFactory() {
        return lookupFactory;
    }

    List<JoinSpecification> getExistsJoins() {
        return existsJoins;
    }

    public QueryProcessor getQueryProcessor() {
        return queryProcessor;
    }

    QueryFactory getQueryFactory() {
        return queryFactory;
    }
}

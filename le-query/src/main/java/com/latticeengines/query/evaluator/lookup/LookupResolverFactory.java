package com.latticeengines.query.evaluator.lookup;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AggregateLookup;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CaseLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.SubQueryAttrLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.evaluator.QueryProcessor;
import com.latticeengines.query.evaluator.restriction.RestrictionResolverFactory;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

public final class LookupResolverFactory {

    private AttributeRepository attrRepo;
    private Map<String, LookupResolver> resolvers = new HashMap<>();
    private RestrictionResolverFactory restrictionResolverFactory;
    private QueryProcessor queryProcessor;

    public LookupResolverFactory(AttributeRepository attrRepo, QueryProcessor queryProcessor) {
        this.attrRepo = attrRepo;
        this.queryProcessor = queryProcessor;
    }

    @SuppressWarnings("unchecked")
    public <T extends Lookup> LookupResolver<T> getLookupResolver(Class<T> lookupType) {
        if (!resolvers.containsKey(lookupType.getSimpleName())) {
            initializeResolver(lookupType);
        }
        return (LookupResolver<T>) resolvers.get(lookupType.getSimpleName());
    }

    private <T extends Lookup> void initializeResolver(Class<T> lookupType) {
        if (lookupType.isAssignableFrom(AttributeLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new AttributeResolver(attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(SubQueryAttrLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new SubQueryAttrResolver(attrRepo, queryProcessor));
            return;
        }
        if (lookupType.isAssignableFrom(EntityLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new EntityResolver(attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(RangeLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new RangeResolver(attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(CollectionLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new CollectionResolver(attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(ValueLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new ValueResolver(attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(AggregateLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new AggregateResolver(attrRepo, this));
            return;
        }
        if (lookupType.isAssignableFrom(CaseLookup.class)) {
            resolvers.put(lookupType.getSimpleName(),
                    new CaseResolver(attrRepo, restrictionResolverFactory));
            return;
        }
        throw new QueryEvaluationException("Do not support lookup of type " + lookupType + " yet.");
    }

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

    public void setRestrictionResolverFactory(RestrictionResolverFactory restrictionResolverFactory) {
        this.restrictionResolverFactory = restrictionResolverFactory;
    }
}

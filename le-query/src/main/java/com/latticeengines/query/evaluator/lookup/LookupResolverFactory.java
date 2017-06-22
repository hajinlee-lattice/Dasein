package com.latticeengines.query.evaluator.lookup;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;
import com.latticeengines.query.util.AttrRepoUtils;

public final class LookupResolverFactory {

    private AttrRepoUtils attrRepoUtils;
    private AttributeRepository attrRepo;
    private Map<String, LookupResolver> resolvers = new HashMap<>();

    public LookupResolverFactory(AttrRepoUtils attrRepoUtils, AttributeRepository attrRepo) {
        this.attrRepoUtils = attrRepoUtils;
        this.attrRepo = attrRepo;
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
            resolvers.put(lookupType.getSimpleName(), new AttributeResolver(attrRepoUtils, attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(EntityLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new EntityResolver(attrRepoUtils, attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(RangeLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new RangeResolver(attrRepoUtils, attrRepo));
            return;
        }
        if (lookupType.isAssignableFrom(ValueLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new ValueResolver(attrRepoUtils, attrRepo));
            return;
        }
        throw new QueryEvaluationException("Do not support lookup of type " + lookupType + " yet.");
    }

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

}

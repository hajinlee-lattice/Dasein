package com.latticeengines.query.evaluator.lookup;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;
import com.latticeengines.query.exposed.exception.QueryEvaluationException;

public final class LookupResolverFactory {
    private AttributeRepository attrRepo;
    private Map<String, LookupResolver> resolvers = new HashMap<>();

    public LookupResolverFactory(AttributeRepository attrRepo) {
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
            resolvers.put(lookupType.getSimpleName(), new AttributeResolver(attrRepo));
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
        if (lookupType.isAssignableFrom(ValueLookup.class)) {
            resolvers.put(lookupType.getSimpleName(), new ValueResolver(attrRepo));
            return;
        }
        throw new QueryEvaluationException("Do not support lookup of type " + lookupType + " yet.");
    }

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

    // below are deprecated
    private Lookup lookup;
    private SchemaInterpretation rootObjectType;
    private DataCollection dataCollection;
    private Lookup secondaryLookup;

    public LookupResolverFactory() {
    }

    public LookupResolverFactory(Builder builder) {
        this.lookup = builder.getLookup();
        this.rootObjectType = builder.getRootObjectType();
        this.dataCollection = builder.getDataCollection();
        this.secondaryLookup = builder.getSecondaryLookup();
    }


    public static Builder builder(AttributeRepository attrRepo) {
        return new Builder().attrRepo(attrRepo);
    }

    public static class Builder {
        private Lookup lookup;
        private SchemaInterpretation rootObjectType;
        private DataCollection dataCollection;
        private Lookup secondaryLookup;
        private AttributeRepository attrRepo;

        public Builder lookup(Lookup lookup) {
            this.lookup = lookup;
            return this;
        }

        private Builder attrRepo(AttributeRepository attrRepo) {
            this.attrRepo = attrRepo;
            return this;
        }

        public Builder rootObjectType(SchemaInterpretation rootObjectType) {
            this.rootObjectType = rootObjectType;
            return this;
        }

        public Builder dataCollection(DataCollection dataCollection) {
            this.dataCollection = dataCollection;
            return this;
        }

        public Builder secondaryLookup(Lookup secondaryLookup) {
            this.secondaryLookup = secondaryLookup;
            return this;
        }

        public Lookup getLookup() {
            return lookup;
        }

        public SchemaInterpretation getRootObjectType() {
            return rootObjectType;
        }

        public DataCollection getDataCollection() {
            return dataCollection;
        }

        public Lookup getSecondaryLookup() {
            return secondaryLookup;
        }

        public LookupResolverFactory build() {
            LookupResolverFactory factory = new LookupResolverFactory();
            factory.attrRepo = this.attrRepo;
            factory.lookup = this.lookup;
            factory.secondaryLookup = this.secondaryLookup;
            return factory;
        }
    }

}

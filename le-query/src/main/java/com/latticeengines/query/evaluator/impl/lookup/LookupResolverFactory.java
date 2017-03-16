package com.latticeengines.query.evaluator.impl.lookup;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.RangeLookup;
import com.latticeengines.domain.exposed.query.ValueLookup;

public class LookupResolverFactory {
    private Lookup lookup;
    private SchemaInterpretation rootObjectType;
    private DataCollection dataCollection;
    private Lookup secondaryLookup;

    public static class Builder {
        private Lookup lookup;
        private SchemaInterpretation rootObjectType;
        private DataCollection dataCollection;
        private Lookup secondaryLookup;

        public Builder lookup(Lookup lookup) {
            this.lookup = lookup;
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
    }

    public LookupResolverFactory(Builder builder) {
        this.lookup = builder.getLookup();
        this.rootObjectType = builder.getRootObjectType();
        this.dataCollection = builder.getDataCollection();
        this.secondaryLookup = builder.getSecondaryLookup();
    }

    public LookupResolver getLookupResolver() {
        if (lookup instanceof ColumnLookup) {
            return new ColumnLookupResolver((ColumnLookup) lookup, rootObjectType, dataCollection);
        } else if (lookup instanceof ValueLookup) {
            return new ValueLookupResolver((ValueLookup) lookup, rootObjectType, dataCollection, secondaryLookup);
        } else if (lookup instanceof RangeLookup) {
            return new RangeLookupResolver((RangeLookup) lookup, rootObjectType, dataCollection, secondaryLookup);
        } else {
            throw new RuntimeException(String.format("Unsupported lookup type %s", lookup.getClass().getName()));
        }
    }
}

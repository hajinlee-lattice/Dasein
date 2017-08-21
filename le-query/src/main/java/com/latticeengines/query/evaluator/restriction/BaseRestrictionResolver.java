package com.latticeengines.query.evaluator.restriction;


import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.query.evaluator.lookup.LookupResolverFactory;
import com.latticeengines.query.util.AttrRepoUtils;

public abstract class BaseRestrictionResolver<T extends Restriction> {

    protected RestrictionResolverFactory factory;
    protected LookupResolverFactory lookupFactory;
    private AttributeRepository attrRepo;

    BaseRestrictionResolver(RestrictionResolverFactory factory) {
        this.lookupFactory = factory.getLookupFactory();
        this.factory = factory;
    }

    protected AttributeRepository getAttrRepo() {
        return this.lookupFactory.getAttrRepo();
    }

    protected AttributeStats findAttributeStats(AttributeLookup attributeLookup) {
        ColumnMetadata attribute = findAttributeMetadata(attributeLookup);
        return attribute.getStats();
    }

    protected ColumnMetadata findAttributeMetadata(AttributeLookup attributeLookup) {
        if (attrRepo == null) {
            attrRepo = getAttrRepo();
        }
        return AttrRepoUtils.getAttribute(attrRepo, attributeLookup);
    }

}

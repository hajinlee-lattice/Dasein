package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Lookup;

public abstract class BaseLookupResolver<T extends Lookup> {

    @Deprecated
    protected DataCollection dataCollection;

    @Deprecated
    protected SchemaInterpretation rootObjectType;

    private AttributeRepository repository;

    BaseLookupResolver(AttributeRepository repository) {
        this.repository = repository;
    }

    Attribute getAttribute(AttributeLookup attributeLookup) {
        return repository.getAttribute(attributeLookup);
    }

}

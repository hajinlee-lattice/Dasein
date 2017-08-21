package com.latticeengines.query.evaluator.lookup;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.query.util.AttrRepoUtils;

public abstract class BaseLookupResolver<T extends Lookup> {

    private AttributeRepository repository;

    BaseLookupResolver(AttributeRepository repository) {
        this.repository = repository;
    }

    ColumnMetadata getColumnMetadata(AttributeLookup attributeLookup) {
        return AttrRepoUtils.getAttribute(repository, attributeLookup);
    }

}

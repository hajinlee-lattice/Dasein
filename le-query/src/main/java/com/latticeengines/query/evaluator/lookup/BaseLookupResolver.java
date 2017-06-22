package com.latticeengines.query.evaluator.lookup;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.query.util.AttrRepoUtils;

public abstract class BaseLookupResolver<T extends Lookup> {

    private AttrRepoUtils attrRepoUtils;

    private AttributeRepository repository;

    BaseLookupResolver(AttrRepoUtils attrRepoUtils, AttributeRepository repository) {
        this.attrRepoUtils = attrRepoUtils;
        this.repository = repository;
    }

    ColumnMetadata getColumnMetadata(AttributeLookup attributeLookup) {
        return attrRepoUtils.getAttribute(repository, attributeLookup);
    }

}

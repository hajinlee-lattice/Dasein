package com.latticeengines.metadata.mds.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.mds.MetadataStoreName;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.metadata.mds.NamedMetadataStore;
import com.latticeengines.metadata.repository.db.AttributeRepository;

import reactor.core.publisher.Flux;

@Component("tableMetadataStore")
public class TableMetadataStore implements NamedMetadataStore<Namespace1<Long>> {

    @Inject
    private AttributeRepository repository;

    @Inject
    private TableTemplate tableTemplate;

    @Override
    public String getName() {
        return MetadataStoreName.Table;
    }

    @Override
    public Long count(Namespace1<Long> namespace) {
        long tablePid = namespace.getCoord1();
        return repository.countByTable_Pid(tablePid);
    }

    @Override
    public Flux<ColumnMetadata> getMetadata(Namespace1<Long> namespace) {
        return tableTemplate.getUnorderedSchema(namespace).sequential();
    }

    @Override
    public Namespace1<Long> parseNameSpace(String... namespace) {
        return tableTemplate.parseNameSpace(namespace);
    }

}

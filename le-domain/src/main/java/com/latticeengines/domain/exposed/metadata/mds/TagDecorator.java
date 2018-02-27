package com.latticeengines.domain.exposed.metadata.mds;

import java.util.Collections;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.Tag;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public class TagDecorator implements Decorator {

    private final Tag tag;

    public TagDecorator(Tag tag) {
        this.tag = tag;
    }

    @Override
    public Flux<ColumnMetadata> render(Flux<ColumnMetadata> metadata) {
        return metadata.map(cm -> {
            cm.setTagList(Collections.singletonList(tag));
            return cm;
        });
    }

    @Override
    public ParallelFlux<ColumnMetadata> render(ParallelFlux<ColumnMetadata> metadata) {
        return metadata.map(cm -> {
            cm.setTagList(Collections.singletonList(tag));
            return cm;
        });
    }

}

package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface ServingStoreService {

    ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version);

    ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity);

    Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity);

}

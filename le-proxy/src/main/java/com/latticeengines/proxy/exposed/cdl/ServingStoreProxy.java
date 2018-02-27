package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;

public interface ServingStoreProxy {

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity);

}

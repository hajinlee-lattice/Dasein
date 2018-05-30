package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ServingStoreProxy {

    Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity);
    Mono<Long> getDecoratedMetadataCount(String customerSpace, BusinessEntity entity, List<ColumnSelection.Predefined> groups);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity);
    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, List<ColumnSelection.Predefined> groups);
    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, List<ColumnSelection
            .Predefined> groups, DataCollection.Version version);

    // only use cache when you have performance control.
    // otherwise using above non-cached apis gives more up-to-date info.
    List<ColumnMetadata> getDecoratedMetadataFromCache(String customerSpace, BusinessEntity entity);

}

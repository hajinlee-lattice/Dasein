package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface ServingStoreService {

    ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version);

    ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version);

    Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity, DataCollection.Version version);

    List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity, DataCollection.Version version,
                                              List<ColumnSelection.Predefined> groups);

    List<ColumnMetadata> getDecoratedMetadata(String customerSpace, List<BusinessEntity> entities, DataCollection.Version version,
                                              List<ColumnSelection.Predefined> groups);

    Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity,
                                                 DataCollection.Version version, Boolean allCustomerAttrs);

    Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
                                                   DataCollection.Version version);

    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity, DataCollection.Version version);
}

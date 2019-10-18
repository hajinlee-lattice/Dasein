package com.latticeengines.apps.cdl.service;

import java.util.Collection;
import java.util.List;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface ServingStoreService {

    // ========== BEGIN: Get Metadata Not From Cache ==========
    ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version);

    ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version);

    List<ColumnMetadata> getDecoratedMetadata(String customerSpace, Collection<BusinessEntity> entities,
            DataCollection.Version version, Collection<ColumnSelection.Predefined> groups, boolean deflateDisplayNames);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Collection<ColumnSelection.Predefined> groups);
    // ========== END: Get Metadata Not From Cache ==========

    // ========== BEGIN: Get Metadata From Cache ==========
    List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, Collection<BusinessEntity> entities, //
            ColumnSelection.Predefined groups, boolean deflateDisplayNames);

    // below are all short-hand method of the generic method above
    List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity);
    // ========== END: Get Metadata From Cache ==========

    // ========== BEGIN: Modeling Attributes ==========
    Flux<ColumnMetadata> getAllowedModelingAttrs(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Boolean allCustomerAttrs);

    Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
            DataCollection.Version version);

    Flux<ColumnMetadata> getNewModelingAttrs(String customerSpace, BusinessEntity entity,
            DataCollection.Version version);
    // ========== END: Modeling Attributes ==========
}

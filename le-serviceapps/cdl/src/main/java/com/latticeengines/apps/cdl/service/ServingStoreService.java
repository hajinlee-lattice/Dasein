package com.latticeengines.apps.cdl.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

public interface ServingStoreService {

    // ========== BEGIN: Get Metadata Not From Cache ==========
    ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version);

    ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version);

    ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version,
            StoreFilter filter);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Collection<ColumnSelection.Predefined> groups);

    Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Collection<ColumnSelection.Predefined> groups, String attributeSetName, StoreFilter filter);

    List<ColumnMetadata> getAccountMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version);

    List<ColumnMetadata> getContactMetadata(String customerSpace, ColumnSelection.Predefined group,
            DataCollection.Version version);

    Map<String, Boolean> getAttributesUsage(String customerSpace, BusinessEntity entity, Set<String> attributes,
            ColumnSelection.Predefined group, DataCollection.Version version);
    // ========== END: Get Metadata Not From Cache ==========

    // ========== BEGIN: Get Metadata From Cache ==========
    List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity);
    // ========== END: Get Metadata From Cache ==========

    // ========== BEGIN: Modeling Attributes ==========
    Flux<ColumnMetadata> getAttrsCanBeEnabledForModeling(String customerSpace, BusinessEntity entity,
            DataCollection.Version version, Boolean allCustomerAttrs);

    Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
            DataCollection.Version version);

    Flux<ColumnMetadata> getAttrsEnabledForModeling(String customerSpace, BusinessEntity entity,
            DataCollection.Version version);
    // ========== END: Modeling Attributes ==========
}

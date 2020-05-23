package com.latticeengines.apps.cdl.service.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.StoreFilter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

class DummyServingStoreService implements ServingStoreService {

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity,
                                                          DataCollection.Version version) {
        return null;
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity,
                                                                  DataCollection.Version version) {
        return null;
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity, DataCollection.Version version, StoreFilter filter, String attributeSetName) {
        return null;
    }

    @Override
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity) {
        return null;
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
                                                     DataCollection.Version version, Collection<ColumnSelection.Predefined> groups) {
        return null;
    }

    @Override
    public Flux<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity,
                                                     DataCollection.Version version, Collection<ColumnSelection.Predefined> groups, String attributeSetName, StoreFilter filter) {
        return null;
    }

    @Override
    public List<ColumnMetadata> getAccountMetadata(String customerSpace, ColumnSelection.Predefined group,
                                                   DataCollection.Version version) {
        return null;
    }

    @Override
    public List<ColumnMetadata> getContactMetadata(String customerSpace, ColumnSelection.Predefined group,
                                                   DataCollection.Version version) {
        return null;
    }

    @Override
    public Flux<ColumnMetadata> getAttrsCanBeEnabledForModeling(String customerSpace, BusinessEntity entity,
                                                                DataCollection.Version version, Boolean allCustomerAttrs) {
        return null;
    }

    @Override
    public Flux<ColumnMetadata> getSystemMetadataAttrFlux(String customerSpace, BusinessEntity entity,
                                                          DataCollection.Version version) {
        return null;
    }

    @Override
    public Flux<ColumnMetadata> getAttrsEnabledForModeling(String customerSpace, BusinessEntity entity,
                                                           DataCollection.Version version) {
        return null;
    }

    @Override
    public Map<String, Boolean> getAttributesUsage(String customerSpace, BusinessEntity entity,
                                                   Set<String> attributes, ColumnSelection.Predefined group, DataCollection.Version version) {
        return null;
    }

}

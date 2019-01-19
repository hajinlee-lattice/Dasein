package com.latticeengines.apps.cdl.service.impl;

import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Service("servingStoreService")
public class ServingStoreServiceImpl implements ServingStoreService {

    @Inject
    private CustomizedMetadataStore customizedMetadataStore;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Inject
    private DataCollectionService dataCollectionService;

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version) {
        return systemMetadataStore.getMetadataInParallel(entity, version);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity,
            DataCollection.Version version) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return getFullyDecoratedMetadata(customerSpace, entity, version);
    }

    @Override
    public Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity,
            DataCollection.Version version) {
        return getFullyDecoratedMetadata(entity, version).sorted(Comparator.comparing(ColumnMetadata::getAttrName));
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.ServingMetadataCacheName, key = "T(java.lang.String).format(\"%s|%s|md_in_svc\", #tenantId, #entity)", unless="#result == null")
    public List<ColumnMetadata> getDecoratedMetadataFromCache(String tenantId, BusinessEntity entity) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        DataCollection.Version version = dataCollectionService.getActiveVersion(customerSpace);
        return getFullyDecoratedMetadata(customerSpace, entity, version).sequential().collectList().block();
    }

    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(String tenantId, BusinessEntity entity,
                                                                  DataCollection.Version version) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        TableRoleInCollection role = entity.getServingStore();
        List<String> tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        if (CollectionUtils.isEmpty(tables) && BusinessEntity.Account.equals(entity)) {
            role = BusinessEntity.Account.getBatchStore();
            tables = dataCollectionService.getTableNames(customerSpace, "", role, version);
        }
        ParallelFlux<ColumnMetadata> flux;
        if (CollectionUtils.isNotEmpty(tables)) {
            flux = customizedMetadataStore.getMetadataInParallel(entity, version).map(cm -> {
                cm.setBitOffset(null);
                cm.setNumBits(null);
                cm.setPhysicalName(null);
                cm.setStatisticalType(null);
                cm.setStats(null);
                cm.setDecodeStrategy(null);

                if (Boolean.TRUE.equals(cm.getShouldDeprecate()) && !AttrState.Inactive.equals(cm.getAttrState())) {
                    cm.setAttrState(AttrState.Deprecated);
                }

                if (AttrState.Inactive.equals(cm.getAttrState())) {
                    // disable these useages if it is inactive attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Segment);
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.TalkingPoint);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                if (AttrState.Deprecated.equals(cm.getAttrState())) {
                    // disable these useages if it is deprecated attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                }

                return cm;
            });
        } else {
            flux = Flux.<ColumnMetadata>empty().parallel();
        }
        return flux;
    }

}

package com.latticeengines.apps.cdl.service.impl;

import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
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
                    cm.disableGroup(ColumnSelection.Predefined.Model);
                }

                if (AttrState.Deprecated.equals(cm.getAttrState())) {
                    // disable these useages if it is deprecated attribute.
                    cm.disableGroup(ColumnSelection.Predefined.Enrichment);
                    cm.disableGroup(ColumnSelection.Predefined.CompanyProfile);
                    cm.disableGroup(ColumnSelection.Predefined.Model);
                }

                return cm;
            });
        } else {
            flux = Flux.<ColumnMetadata>empty().parallel();
        }
        return flux;
    }

    @Override
    public Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity,
            DataCollection.Version version) {
        return getFullyDecoratedMetadata(entity, version).sorted(Comparator.comparing(ColumnMetadata::getAttrName));
    }

}

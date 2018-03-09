package com.latticeengines.apps.cdl.service.impl;

import java.util.Comparator;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedMetadataStore;
import com.latticeengines.apps.cdl.mds.SystemMetadataStore;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Service("servingStoreService")
public class ServingStoreServiceImpl implements ServingStoreService {

    @Inject
    private CustomizedMetadataStore customizedMetadataStore;

    @Inject
    private SystemMetadataStore systemMetadataStore;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version) {
        return systemMetadataStore.getMetadataInParallel(entity, version);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        ParallelFlux<ColumnMetadata> pFlux = Flux.<ColumnMetadata>empty().parallel();
        if (role != null) {
            String tenantId = MultiTenantContext.getTenantId();
            String tableName = dataCollectionProxy.getTableName(tenantId, role);
            if (StringUtils.isNotBlank(tableName)) {
                pFlux = customizedMetadataStore.getMetadataInParallel(entity).map(cm -> {
                    cm.setBitOffset(null);
                    cm.setNumBits(null);
                    cm.setPhysicalName(null);
                    cm.setStatisticalType(null);
                    cm.setStats(null);
                    cm.setDecodeStrategy(null);
                    return cm;
                });
            }
        }
        return pFlux;
    }

    @Override
    public Flux<ColumnMetadata> getFullyDecoratedMetadataInOrder(BusinessEntity entity) {
        return getFullyDecoratedMetadata(entity).sorted(Comparator.comparing(ColumnMetadata::getAttrName));
    }

}

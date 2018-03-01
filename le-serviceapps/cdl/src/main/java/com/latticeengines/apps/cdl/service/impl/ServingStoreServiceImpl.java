package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.mds.CustomizedServingStoreTemplate;
import com.latticeengines.apps.cdl.mds.SystemServingStoreTemplate;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;

import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;

@Service("servingStoreService")
public class ServingStoreServiceImpl implements ServingStoreService {

    @Inject
    private CustomizedServingStoreTemplate customizedTemplate;

    @Inject
    private SystemServingStoreTemplate systemServingStoreTemplate;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Override
    public ParallelFlux<ColumnMetadata> getSystemMetadata(BusinessEntity entity, DataCollection.Version version) {
        return systemServingStoreTemplate.getUnorderedSchema(entity, version);
    }

    @Override
    public ParallelFlux<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        ParallelFlux<ColumnMetadata> pFlux = Flux.<ColumnMetadata>empty().parallel();
        if (role != null) {
            String tenantId = MultiTenantContext.getTenantId();
            String tableName = dataCollectionProxy.getTableName(tenantId, role);
            if (StringUtils.isNotBlank(tableName)) {
                pFlux = customizedTemplate.getUnorderedSchema(entity).map(cm -> {
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

}

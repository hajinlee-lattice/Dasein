package com.latticeengines.metadata.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.metadata.service.SegmentationDataCollectionService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("segmentationDataCollectionService")
public class SegmentationDataCollectionServiceImpl implements SegmentationDataCollectionService {

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public DataCollection getDefaultDataCollection() {
        DataCollection collection = new DataCollection();
        collection.setName("Default");
        collection.setType(DataCollectionType.Segmentation);
        return collection;
    }

    @Override
    public void fillInDefaultTables(DataCollection dataCollection) {
        if (dataCollection.getTable(SchemaInterpretation.BucketedAccountMaster) == null) {
            Tenant tenant = MultiTenantContext.getTenant();
            try {
                MultiTenantContext.setTenant(tenantEntityMgr.findByTenantId(DataCloudConstants.SERVICE_CUSTOMERSPACE));
                Table table = metadataService.getTable(CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE),
                        DataCloudConstants.BUCKETED_ACCOUNT_MASTER_TABLE_NAME);
                if (table != null) {
                    dataCollection.addTable(table);
                }
            } finally {
                MultiTenantContext.setTenant(tenant);
            }
        }
    }
}

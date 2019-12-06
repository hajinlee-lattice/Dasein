package com.latticeengines.apps.cdl.mds.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ActivityMetricsGroupEntityMgr;
import com.latticeengines.apps.cdl.mds.ActivityMetricDecoratorFac;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.DimensionMetadataService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.DummyDecorator;
import com.latticeengines.domain.exposed.metadata.namespace.Namespace1;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("activityMetricDecorator")
public class ActivityMetricDecoratorFacImpl implements ActivityMetricDecoratorFac {

    @Inject
    private BatonService batonService;

    @Inject
    private DimensionMetadataService dimensionMetadataService;

    @Inject
    private ActivityMetricsGroupEntityMgr activityMetricsGroupEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public Decorator getDecorator(Namespace1<String> namespace) {
        String tenantId = namespace.getCoord1();
        if (StringUtils.isNotBlank(tenantId)) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
            if (entityMatchEnabled) {
                String customerSpace = CustomerSpace.parse(tenantId).toString();
                DataCollection dataCollection = dataCollectionService.getDefaultCollection(customerSpace);
                DataCollection.Version version = dataCollection.getVersion();
                DataCollectionStatus status = //
                        dataCollectionService.getOrCreateDataCollectionStatus(customerSpace, version);
                String signature = status.getDimensionMetadataSignature();
                Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(tenantId).toString());
                return new ActivityMetricDecorator(signature, tenant, dimensionMetadataService,
                        activityMetricsGroupEntityMgr);
            }
        }
        return new DummyDecorator();
    }
}

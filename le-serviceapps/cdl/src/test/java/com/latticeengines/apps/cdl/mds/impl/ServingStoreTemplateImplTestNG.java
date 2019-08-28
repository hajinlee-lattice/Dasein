package com.latticeengines.apps.cdl.mds.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.apps.core.mds.AttrConfigDecorator;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.mds.Decorator;
import com.latticeengines.domain.exposed.metadata.mds.MapDecorator;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.ParallelFlux;

public class ServingStoreTemplateImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private ProxyResourceService proxyResourceService;

    @Inject
    private AttrConfigDecorator attrConfigDecorator;

    @Test(groups = "manual", enabled = false)
    public void testDecorator() {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse("LocalTest").toString());
        MultiTenantContext.setTenant(tenant);
        Decorator decorator = attrConfigDecorator.getDecorator(tenant.getId(), BusinessEntity.Account);
        MapDecorator mapDecorator = (MapDecorator) decorator;
        mapDecorator.load();
    }

    @Test(groups = "manual", enabled = true)
    public void testGetMetadata() throws InterruptedException {
        Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse("LocalTest").toString());
        MultiTenantContext.setTenant(tenant);
        loadViaMds();
        Thread.sleep(500);
        loadViaLegacyWay();
        Thread.sleep(500);
        loadViaMds();
    }

    private void loadViaMds() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = proxyResourceService.getActiveVersion(customerSpace);
        try (PerformanceTimer timer = new PerformanceTimer("Load full serving store schema.")) {
            ParallelFlux<ColumnMetadata> pFlux = servingStoreService.getFullyDecoratedMetadata(
                    BusinessEntity.Account, active);
            List<ColumnMetadata> cms = pFlux.sequential().collectList().block();
            Assert.assertNotNull(cms);
            Assert.assertEquals(cms.size(), 21734L);
        }
    }

    private void loadViaLegacyWay() {
        try (PerformanceTimer timer = new PerformanceTimer("Load full data collection table.")) {
            Table table = proxyResourceService.getTable(MultiTenantContext.getCustomerSpace().toString(),
                    TableRoleInCollection.BucketedAccount);
            Assert.assertNotNull(table);
            List<ColumnMetadata> cms = table.getAttributes().stream().map(Attribute::getColumnMetadata).collect(Collectors.toList());
            Assert.assertEquals(cms.size(), 21734L);
        }
    }

}

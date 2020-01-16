package com.latticeengines.apps.cdl.mds.impl;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DataCollectionService;
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
import com.latticeengines.domain.exposed.query.StoreFilter;
import com.latticeengines.domain.exposed.security.Tenant;

import reactor.core.publisher.ParallelFlux;

public class ServingStoreTemplateImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private DataCollectionService dataCollectionService;

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
        loadViaMdsWithoutLDC();
        Thread.sleep(500);
        loadViaLegacyWay();
        Thread.sleep(500);
        loadViaMds();
    }

    private void loadViaMds() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = dataCollectionService.getActiveVersion(customerSpace);
        try (PerformanceTimer timer = new PerformanceTimer("Load full serving store schema.")) {
            ParallelFlux<ColumnMetadata> pFlux = servingStoreService.getFullyDecoratedMetadata(
                    BusinessEntity.Account, active);
            List<ColumnMetadata> cms = pFlux.sequential().collectList().block();
            Assert.assertNotNull(cms);
            Assert.assertEquals(cms.size(), 37861L);
        }
    }

    private void loadViaMdsWithoutLDC() {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        DataCollection.Version active = dataCollectionService.getActiveVersion(customerSpace);
        try (PerformanceTimer timer = new PerformanceTimer("Load serving store without LDC schema.")) {
            ParallelFlux<ColumnMetadata> pFlux = servingStoreService.getFullyDecoratedMetadata(
                    BusinessEntity.Account, active, StoreFilter.NO_LDC);
            List<ColumnMetadata> cms = pFlux.sequential().collectList().block();
            Assert.assertNotNull(cms);
            Assert.assertEquals(cms.size(), 23);
        }
    }

    private void loadViaLegacyWay() {
        try (PerformanceTimer timer = new PerformanceTimer("Load full data collection table.")) {
            Table table = dataCollectionService.getTable(MultiTenantContext.getCustomerSpace().toString(),
                    TableRoleInCollection.BucketedAccount, null);
            Assert.assertNotNull(table);
            List<ColumnMetadata> cms = table.getAttributes().stream().map(Attribute::getColumnMetadata).collect(Collectors.toList());
            Assert.assertEquals(cms.size(), 21734L);
        }
    }

}

package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.util.List;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.objectapi.service.PurchaseHistoryService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-objectapi-context.xml" })
public class PurchaseHistoryServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private Tenant tenant;

    @Inject
    private PurchaseHistoryService purchaseHistoryService;

    @BeforeClass(groups = "manual")
    protected void setup() {
        tenant = new Tenant("testFileImport_0518_03");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
        mockDataCollectionProxy();
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getTableName(any(), any()))
                .thenReturn("\"app\".\"public\".\"fs_ds_test_3_account_2018_06_05_18_34_34_utc\"");
        // NOTE set redshift partition in status
        Mockito.when(proxy.getOrCreateDataCollectionStatus(Mockito.anyString(), Mockito.any()))
                .thenReturn(new DataCollectionStatus());
        ((PurchaseHistoryServiceImpl) purchaseHistoryService).setDataCollectionProxy(proxy);
    }

    @Test(groups = "manual")
    public void testGetAllSpendAnalyticsSegments() {
        DataPage result = purchaseHistoryService.getAllSpendAnalyticsSegments();
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getData().size(), 3);
    }

    @Test(groups = "manual")
    public void TestGetPeriodTransactionsForSegmentAccounts() {
        List<PeriodTransaction> results = purchaseHistoryService.getPeriodTransactionsForSegmentAccounts("SpendSegment",
                "Month", ProductType.Spending);
    }

}

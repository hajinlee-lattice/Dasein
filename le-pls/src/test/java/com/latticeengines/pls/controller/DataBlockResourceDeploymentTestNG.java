package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlock;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockMetadataContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.pls.functionalframework.DCPDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.proxy.pls.TestDataBlockProxy;

public class DataBlockResourceDeploymentTestNG extends DCPDeploymentTestNGBase {

    @Inject
    private TestDataBlockProxy testDataBlockProxy;

    @BeforeClass(groups = "deployment-dcp")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        attachProtectedProxy(testDataBlockProxy);
    }

    @Test(groups = "deployment-dcp")
    public void testGetBlockMetadata() {
        DataBlockMetadataContainer container = testDataBlockProxy.getBlockMetadata();
        Assert.assertNotNull(container);
        DataBlock block = container.getBlocks().get("companyfinancials");
        Assert.assertNotNull(block);
        Assert.assertTrue(block.getBlockName().equals( "Company Financials"));
    }

    @Test(groups = "deployment-dcp")
    public void testGetElements() {
        List<DataBlock> blocks = testDataBlockProxy.getBlocks();
        Assert.assertTrue(!blocks.isEmpty());

        DataBlock block = null;

        for (int index = 0; index < blocks.size(); index++) {
            DataBlock thisBlock = blocks.get(index);
            if (thisBlock.getBlockId().equals("companyfinancials")) {
                block = thisBlock;
            }
        }

        Assert.assertNotNull(block);
        Assert.assertEquals(block.getBlockName(), "Company Financials");
        Assert.assertTrue(block.getLevels().size() == 1);
        Assert.assertTrue(block.getLevels().get(0).getLevel() == DataBlockLevel.L1);
    }

    @Test(groups = "deployment-dcp")
    public void testGetEntitlements() {
        DataBlockEntitlementContainer container = testDataBlockProxy.getEntitlement();
        Assert.assertNotNull(container);

        DataBlockEntitlementContainer.Domain domain = container.getDomains().get(0);
        Assert.assertNotNull(domain);

        DataDomain dataDomain = domain.getDomain();
        Assert.assertNotNull(dataDomain);
        Assert.assertTrue(dataDomain == DataDomain.SalesMarketing);

        Assert.assertTrue(container.getDomains().get(0).getDomain() == DataDomain.SalesMarketing);
    }
}

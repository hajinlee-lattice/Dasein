package com.latticeengines.apps.dcp.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataRecordType.Analytical;
import static com.latticeengines.domain.exposed.datacloud.manage.DataRecordType.Domain;
import static com.latticeengines.domain.exposed.datacloud.manage.DataRecordType.MasterData;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

public class EntitlementServiceImplUnitTestNG {

    @Test(groups = "unit")
    public void testDefaultEntitlement() {
        DataBlockEntitlementContainer container = EntitlementServiceImpl.getDefaultEntitlement();
        Assert.assertNotNull(container);
        Assert.assertEquals(container.getDomains().size(), 1);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().size(), 1);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().get(Domain).size(), 3);
    }

    @Test(groups = "unit")
    public void parseIDaaSEntitlement() throws IOException {
        InputStream is = new ClassPathResource("append-config/idaas-entitlement.json").getInputStream();
        String idaasStr = IOUtils.toString(is, Charset.defaultCharset());
        DataBlockEntitlementContainer container = EntitlementServiceImpl.parseIDaaSEntitlement(idaasStr);
        Assert.assertNotNull(container);
        Assert.assertEquals(container.getDomains().size(), 2);
        for (DataBlockEntitlementContainer.Domain domain: container.getDomains()) {
            switch (domain.getDomain()) {
                case SalesMarketing:
                    Assert.assertEquals(domain.getRecordTypes().size(), 2);
                    Assert.assertTrue(domain.getRecordTypes().containsKey(Domain));
                    Assert.assertTrue(domain.getRecordTypes().containsKey(MasterData));
                    Assert.assertEquals(domain.getRecordTypes().get(Domain).size(), 9);
                    Assert.assertEquals(domain.getRecordTypes().get(MasterData).size(), 5);
                    break;
                case Finance:
                    Assert.fail("Should not see Finance domain with only Analytical Use entitlement");
                    break;
                case Supply:
                    Assert.assertEquals(domain.getRecordTypes().size(), 1);
                    Assert.assertTrue(domain.getRecordTypes().containsKey(MasterData));
                    Assert.assertEquals(domain.getRecordTypes().get(MasterData).size(), 5);
                    Assert.assertFalse(domain.getRecordTypes().containsKey(Analytical));
                    break;
                default:
                    Assert.fail("Should not see domain " + domain.getDomain());
            }
        }

    }

    @Test(groups = "unit")
    public void parseAnalyticalOnly() throws IOException {
        InputStream is = new ClassPathResource("append-config/idaas-entitlement-2.json").getInputStream();
        String idaasStr = IOUtils.toString(is, Charset.defaultCharset());
        DataBlockEntitlementContainer container = EntitlementServiceImpl.parseIDaaSEntitlement(idaasStr);
        Assert.assertNotNull(container);
        Assert.assertTrue(container.getDomains().isEmpty());
    }

    @Test(groups = "unit")
    public void filterDataBlockLevels() throws Exception {
        InputStream is = new ClassPathResource("append-config/idaas-entitlement.json").getInputStream();
        String idaasStr = IOUtils.toString(is, Charset.defaultCharset());
        DataBlockEntitlementContainer container = EntitlementServiceImpl.parseIDaaSEntitlement(idaasStr);
        Assert.assertNotNull(container);
        DataBlockEntitlementContainer filteredContainer = EntitlementServiceImpl
                .filterFinancialDataBlockLevels(container);
        Assert.assertNotNull(filteredContainer);

        List<DataBlockEntitlementContainer.Block> blocks = new ArrayList<>();

        for (DataBlockEntitlementContainer.Domain domain : filteredContainer.getDomains()) {
            if (domain.getDomain().equals(DataDomain.Finance)) {

                for (Map.Entry<DataRecordType, List<DataBlockEntitlementContainer.Block>> entry : domain
                        .getRecordTypes().entrySet()) {

                    for (DataBlockEntitlementContainer.Block block : entry.getValue()) {
                        boolean includeBlock = false;

                        for (DataBlockLevel level : block.getDataBlockLevels()) {
                            if (!level.equals(DataBlockLevel.L1)) {
                                includeBlock = true;
                                break;
                            }
                        }

                        if (includeBlock) {
                            blocks.add(block);
                        }
                    }
                }
            }
        }

        Assert.assertTrue(blocks.isEmpty());
    }

}

package com.latticeengines.apps.dcp.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataRecordType.Domain;
import static com.latticeengines.domain.exposed.datacloud.manage.DataRecordType.MasterData;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

public class AppendConfigServiceImplUnitTestNG {

    @Test(groups = "unit", enabled = false)
    public void testDefaultEntitlement() {
        DataBlockEntitlementContainer container = AppendConfigServiceImpl.getDefaultEntitlement();
        Assert.assertNotNull(container);
        Assert.assertEquals(container.getDomains().size(), 1);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().size(), 2);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().get(Domain).size(), 3);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().get(MasterData).size(), 3);
    }

    @Test(groups = "unit", enabled = false)
    public void parseIDaaSEntitlement() throws IOException {
        InputStream is = new ClassPathResource("append-config/idaas-entitlement.json").getInputStream();
        String idaasStr = IOUtils.toString(is, Charset.defaultCharset());
        DataBlockEntitlementContainer container = AppendConfigServiceImpl.parseIDaaSEntitlement(idaasStr);
        Assert.assertNotNull(container);
        Assert.assertEquals(container.getDomains().size(), 1);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().size(), 1);
        Assert.assertEquals(container.getDomains().get(0).getRecordTypes().get(Domain).size(), 10);
    }

}

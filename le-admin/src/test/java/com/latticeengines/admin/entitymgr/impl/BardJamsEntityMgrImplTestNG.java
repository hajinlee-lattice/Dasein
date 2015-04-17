package com.latticeengines.admin.entitymgr.impl;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.domain.exposed.admin.BardJamsTenantStatus;
import com.latticeengines.domain.exposed.admin.BardJamsTenant;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class BardJamsEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Test(groups = "functional")
    public void testCRUD() {
        BardJamsTenant tenant = getBardJamsTenant();
        bardJamsEntityMgr.create(tenant);
        Assert.assertNotNull(tenant.getPid());

        BardJamsTenant newTenant = bardJamsEntityMgr.findByKey(tenant);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getPid(), tenant.getPid());

         bardJamsEntityMgr.delete(newTenant);
        
         newTenant = bardJamsEntityMgr.findByKey(tenant);
         Assert.assertNull(newTenant);

    }

    private BardJamsTenant getBardJamsTenant() {
        BardJamsTenant tenant = new BardJamsTenant();
        tenant.setTenant("newTenant3");

        tenant.setDlUrl("https://dataloader-prod.lattice-engines.com/Dataloader_PLS/");
        tenant.setDlUser("admin.dataloader@lattice-engines.com");
        tenant.setDlPassword("adm1nDLpr0d");
        tenant.setImmediateFolderStruct("DanteTesting\\Immediate\\");
        tenant.setScheduledFolderStruct("DataLoader\\DL TEST\\Scheduled Jobs");
        tenant.setAgentName("10.41.1.247");
        tenant.setTenantType("P");

        tenant.setStatus(BardJamsTenantStatus.NEW.getStatus());
        return tenant;
    }
}

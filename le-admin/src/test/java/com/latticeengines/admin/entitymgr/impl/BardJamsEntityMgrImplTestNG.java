package com.latticeengines.admin.entitymgr.impl;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.BardJamsEntityMgr;
import com.latticeengines.domain.exposed.admin.BardJamsRequestStatus;
import com.latticeengines.domain.exposed.admin.BardJamsTenants;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class BardJamsEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private BardJamsEntityMgr bardJamsEntityMgr;

    @Test(groups = "functional")
    public void testCRUD() {
        BardJamsTenants tenant = getBardJamsTenant();
        bardJamsEntityMgr.create(tenant);
        Assert.assertNotNull(tenant.getPid());

        BardJamsTenants newTenant = bardJamsEntityMgr.findByKey(tenant);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getPid(), tenant.getPid());

         bardJamsEntityMgr.delete(newTenant);
        
         newTenant = bardJamsEntityMgr.findByKey(tenant);
         Assert.assertNull(newTenant);

    }

    private BardJamsTenants getBardJamsTenant() {
        BardJamsTenants request = new BardJamsTenants();
        request.setTenant("newTenant3");

        request.setDlUrl("https://dataloader-prod.lattice-engines.com/Dataloader_PLS/");
        request.setDlUser("admin.dataloader@lattice-engines.com");
        request.setDlPassword("adm1nDLpr0d");
        request.setImmediateFolderStruct("DanteTesting\\Immediate\\");
        request.setScheduledFolderStruct("DataLoader\\DL TEST\\Scheduled Jobs");
        request.setAgentName("10.41.1.247");
        request.setTenantType("P");

        request.setStatus(BardJamsRequestStatus.NEW.getStatus());
        return request;
    }
}

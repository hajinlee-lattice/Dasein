package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.EnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.EnrichmentAttrEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class EnrichmentAttrEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private EnrichmentAttrEntityMgr entityMgr;

    private Tenant tenant1;
    private Tenant tenant2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithGATenants(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);
    }

    @Test(groups = "functional")
    public void upsert() {
        setupSecurityContext(tenant1);

        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should be no attribute in the beginning");

        List<EnrichmentAttribute> attrs = new ArrayList<>();
        attrs.add(new EnrichmentAttribute("column1", tenant1));
        attrs.add(new EnrichmentAttribute("column2", tenant1));
        entityMgr.upsert(attrs);

        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");

        attrs = new ArrayList<>();
        attrs.add(new EnrichmentAttribute("column1", tenant1));
        attrs.add(new EnrichmentAttribute("column3", tenant1));
        entityMgr.upsert(attrs);

        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should still be 2 attribute after upserting");

        entityMgr.upsert(new ArrayList<EnrichmentAttribute>());

        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should still be 0 attribute in the end");
    }

    @Test(groups = "functional", dependsOnMethods = { "upsert" })
    public void tenantSafe() {
        setupSecurityContext(tenant1);
        List<EnrichmentAttribute> attrs = new ArrayList<>();
        attrs.add(new EnrichmentAttribute("column1", tenant1));
        attrs.add(new EnrichmentAttribute("column2", tenant1));
        entityMgr.upsert(attrs);
        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");

        setupSecurityContext(tenant2);
        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should 0 attribute in the other tenant");
    }

}

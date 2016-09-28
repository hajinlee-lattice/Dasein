package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.SelectedAttribute;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SelectedAttrEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class SelectedAttrEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private SelectedAttrEntityMgr entityMgr;

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
        entityMgr.upsert(new ArrayList<SelectedAttribute>(), new ArrayList<SelectedAttribute>());

        setupSecurityContext(tenant1);

        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should be no attribute in the beginning");

        List<SelectedAttribute> attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column1", tenant1));
        attrs.add(new SelectedAttribute("column2", tenant1));
        entityMgr.upsert(attrs, new ArrayList<SelectedAttribute>());

        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");

        attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column1", tenant1));
        attrs.add(new SelectedAttribute("column3", tenant1));
        entityMgr.upsert(attrs, new ArrayList<SelectedAttribute>());

        Assert.assertEquals(entityMgr.findAll().size(), 3, "There should still be 3 attribute after upserting");

        attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column2", tenant1));
        attrs.add(new SelectedAttribute("column3", tenant1));
        entityMgr.upsert(new ArrayList<SelectedAttribute>(), attrs);

        Assert.assertEquals(entityMgr.findAll().size(), 1, "There should still be 1 attribute in the end");

        attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column4", tenant1));

        List<SelectedAttribute> delAttrs = new ArrayList<>();
        delAttrs.add(new SelectedAttribute("column1", tenant1));
        entityMgr.upsert(attrs, delAttrs);

        Assert.assertEquals(entityMgr.findAll().size(), 1, "There should still be 1 attribute in the end");

        delAttrs = new ArrayList<>();
        delAttrs.add(new SelectedAttribute("column4", tenant1));
        entityMgr.upsert(new ArrayList<SelectedAttribute>(), delAttrs);

        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should still be 0 attribute in the end");

    }

    @Test(groups = "functional", dependsOnMethods = { "upsert" })
    public void tenantSafe() {
        setupSecurityContext(tenant1);
        List<SelectedAttribute> attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column1", tenant1));
        attrs.add(new SelectedAttribute("column2", tenant1));
        entityMgr.upsert(attrs, new ArrayList<SelectedAttribute>());
        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");

        setupSecurityContext(tenant2);
        Assert.assertEquals(entityMgr.findAll().size(), 0, "There should 0 attribute in the other tenant");
    }

    @Test(groups = "functional", dependsOnMethods = { "tenantSafe" })
    public void add() {
        setupSecurityContext(tenant1);
        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");
        List<SelectedAttribute> attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column1", tenant1));
        attrs.add(new SelectedAttribute("column2", tenant1));
        entityMgr.add(attrs);
        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after inserting");

        attrs = new ArrayList<>();
        attrs.add(new SelectedAttribute("column3", tenant1));
        attrs.add(new SelectedAttribute("column4", tenant1));

        entityMgr.add(attrs);
        Assert.assertEquals(entityMgr.findAll().size(), 4, "There should be 4 attribute after inserting");
    }

    @Test(groups = "functional", dependsOnMethods = { "add" })
    public void delete() {
        setupSecurityContext(tenant1);
        Assert.assertEquals(entityMgr.findAll().size(), 4, "There should be 4 attribute");
        List<SelectedAttribute> deleteAttrs = new ArrayList<>();
        deleteAttrs.add(new SelectedAttribute("column2", tenant1));
        deleteAttrs.add(new SelectedAttribute("column3", tenant1));

        List<SelectedAttribute> remainingAttrs = entityMgr.delete(deleteAttrs);

        List<SelectedAttribute> expectedRemainingAttr = new ArrayList<>();
        expectedRemainingAttr.add(new SelectedAttribute("column1", tenant1));
        expectedRemainingAttr.add(new SelectedAttribute("column4", tenant1));

        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute after deleting");

        for (SelectedAttribute remainingAttr : remainingAttrs) {
            Assert.assertTrue(expectedRemainingAttr.contains(remainingAttr));
            Assert.assertFalse(deleteAttrs.contains(remainingAttr));
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "delete" })
    public void count() {
        setupSecurityContext(tenant1);
        Assert.assertEquals(entityMgr.findAll().size(), 2, "There should be 2 attribute");
        boolean onlyPremium = false;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 2);

        onlyPremium = true;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 0);

        List<SelectedAttribute> premiumAttrs = new ArrayList<>();
        premiumAttrs.add(new SelectedAttribute("column5", tenant1, true));
        premiumAttrs.add(new SelectedAttribute("column6", tenant1, true));

        entityMgr.add(premiumAttrs);

        onlyPremium = false;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 4);

        onlyPremium = true;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 2);

        List<SelectedAttribute> deleteAttrs = new ArrayList<>();
        deleteAttrs.add(new SelectedAttribute("column1", tenant1, true));
        deleteAttrs.add(new SelectedAttribute("column6", tenant1, true));

        entityMgr.delete(deleteAttrs);

        onlyPremium = false;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 2);

        onlyPremium = true;
        Assert.assertEquals(entityMgr.count(onlyPremium).intValue(), 1);

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws Exception {
        setupSecurityContext(tenant1);
        entityMgr.upsert(new ArrayList<SelectedAttribute>(), entityMgr.findAll());
        Assert.assertTrue(entityMgr.findAll().size() == 0);
    }

}

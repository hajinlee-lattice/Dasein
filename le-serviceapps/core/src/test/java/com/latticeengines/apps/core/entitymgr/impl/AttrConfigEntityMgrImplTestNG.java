package com.latticeengines.apps.core.entitymgr.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AttrConfigEntityMgrImplTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Test(groups = "functional")
    public void testCrud() throws InterruptedException {
        String tenantName = TestFrameworkUtils.generateTenantName();
        BusinessEntity entity = BusinessEntity.Account;

        AttrConfig attrConfig1 = new AttrConfig();
        attrConfig1.setAttrName("Attr1");
        AttrConfig attrConfig2 = new AttrConfig();
        attrConfig2.setAttrName("Attr2");
        AttrConfig attrConfig3 = new AttrConfig();
        attrConfig3.setAttrName("Attr3");

        List<AttrConfig> attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));

        attrConfigEntityMgr.save(tenantName, entity, Arrays.asList(attrConfig1, attrConfig2, attrConfig3));
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);

        AttrConfigProp attrConfigProp = new AttrConfigProp();
        attrConfigProp.setCustomValue("Display Name 1");
        attrConfig2.putProperty(ColumnMetadataKey.DisplayName, attrConfigProp);
        AttrConfig attrConfig4 = new AttrConfig();
        attrConfig4.setAttrName("Attr4");
        List<AttrConfigEntity> response = attrConfigEntityMgr.save(tenantName, entity, Arrays.asList(attrConfig2, attrConfig4));
        Assert.assertEquals(response.size(), 2);
        response.forEach(entity1 -> Assert.assertNotNull(entity1.getCreatedDate()));
        response.forEach(entity1 -> Assert.assertNotNull(entity1.getLastModifiedDate()));
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertEquals(attrConfigs.size(), 4);
        attrConfigs.forEach(attrConfig -> {
            if (attrConfig.getAttrName().equals("Attr2")) {
                Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnMetadataKey.DisplayName));
                Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getCustomValue(), "Display Name 1");
            }
        });

        attrConfigEntityMgr.delete(tenantName, entity);
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));
    }


    @Test(groups = "functional")
    public void testCleanupTenant() throws InterruptedException {
        String tenant1 = TestFrameworkUtils.generateTenantName();
        Thread.sleep(10);
        String tenant2 = TestFrameworkUtils.generateTenantName();
        BusinessEntity entity = BusinessEntity.Account;

        AttrConfig attrConfig1 = new AttrConfig();
        attrConfig1.setAttrName("Attr1");
        AttrConfig attrConfig2 = new AttrConfig();
        attrConfig2.setAttrName("Attr2");
        AttrConfig attrConfig3 = new AttrConfig();
        attrConfig3.setAttrName("Attr3");

        attrConfigEntityMgr.save(tenant1, entity, Arrays.asList(attrConfig1, attrConfig2, attrConfig3));
        attrConfigEntityMgr.save(tenant2, entity, Arrays.asList(attrConfig1, attrConfig2, attrConfig3));
        Thread.sleep(500); // wait for replication lag

        List<AttrConfig> attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant1, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant2, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);

        attrConfigEntityMgr.cleanupTenant(tenant1);
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant1, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant2, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);

        attrConfigEntityMgr.cleanupTenant(TestFrameworkUtils.generateTenantName());
        attrConfigEntityMgr.save(tenant2, BusinessEntity.Contact, Arrays.asList(attrConfig1, attrConfig2, attrConfig3));
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant2, BusinessEntity.Contact);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);

        attrConfigEntityMgr.cleanupTenant(tenant2);
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant2, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenant2, BusinessEntity.Contact);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));
    }

}

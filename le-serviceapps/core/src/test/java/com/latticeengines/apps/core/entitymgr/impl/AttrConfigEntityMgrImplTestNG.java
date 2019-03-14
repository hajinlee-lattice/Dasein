package com.latticeengines.apps.core.entitymgr.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.testframework.ServiceAppsFunctionalTestNGBase;
import com.latticeengines.domain.exposed.metadata.ColumnMetadataKey;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AttrConfigEntityMgrImplTestNG extends ServiceAppsFunctionalTestNGBase {

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    private static final String CHN_DISPALY_NAME = "Display Name 1 你好";
    private static final String EUR_DISPALY_NAME = "â, ê, î, ô, û";

    private String tenantName;
    private String tenant1;
    private String tenant2;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        Arrays.asList(tenantName, tenant1, tenant2).forEach(t -> {
            attrConfigEntityMgr.cleanupTenant(t);
        });
    }

    @Test(groups = "functional")
    public void testCrud() throws InterruptedException {
        tenantName = TestFrameworkUtils.generateTenantName();
        BusinessEntity entity = BusinessEntity.Account;

        AttrConfig attrConfig1 = new AttrConfig();
        attrConfig1.setAttrName("test_Attr1");
        attrConfig1.setAttrProps(new HashMap<>());
        AttrConfig attrConfig2 = new AttrConfig();
        attrConfig2.setAttrName("Attr2");
        attrConfig2.setAttrProps(new HashMap<>());
        AttrConfig attrConfig3 = new AttrConfig();
        attrConfig3.setAttrName("Attr3");
        attrConfig3.setAttrProps(new HashMap<>());

        List<AttrConfig> attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));

        attrConfigEntityMgr.save(tenantName, entity, Arrays.asList(attrConfig1, attrConfig2, attrConfig3));
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 3);

        AttrConfigProp<String> attrConfigProp2 = new AttrConfigProp<>();
        attrConfigProp2.setCustomValue(CHN_DISPALY_NAME);
        attrConfig2.putProperty(ColumnMetadataKey.DisplayName, attrConfigProp2);
        AttrConfig attrConfig4 = new AttrConfig();
        attrConfig4.setAttrName("Attr4");
        AttrConfigProp<String> attrConfigProp4 = new AttrConfigProp<>();
        attrConfigProp4.setCustomValue(EUR_DISPALY_NAME);
        attrConfig4.putProperty(ColumnMetadataKey.DisplayName, attrConfigProp4);
        List<AttrConfig> response = attrConfigEntityMgr.save(tenantName, entity,
                Arrays.asList(attrConfig2, attrConfig4));
        Assert.assertEquals(response.size(), 2);
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertEquals(attrConfigs.size(), 4);
        attrConfigs.forEach(attrConfig -> {
            // if (attrConfig.getAttrName().equals("Attr2")) {
            // Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnMetadataKey.DisplayName));
            // Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getCustomValue(),
            // CHN_DISPALY_NAME);
            // }
            // if (attrConfig.getAttrName().equals("Attr4")) {
            // Assert.assertTrue(attrConfig.getAttrProps().containsKey(ColumnMetadataKey.DisplayName));
            // Assert.assertEquals(attrConfig.getAttrProps().get(ColumnMetadataKey.DisplayName).getCustomValue(),
            // EUR_DISPALY_NAME);
            // }
        });

        attrConfigs = attrConfigEntityMgr.findAllHaveCustomDisplayNameByTenantId(tenantName);
        Assert.assertEquals(response.size(), 2);
        Assert.assertTrue(attrConfigs.stream().allMatch(attr -> attr.getAttrName().equals(attrConfig2.getAttrName())
                || attr.getAttrName().equals(attrConfig4.getAttrName())));

        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 4);

        attrConfigs = attrConfigEntityMgr.findAllInEntitiesInReader(tenantName, Collections.singletonList(entity));
        Assert.assertEquals(attrConfigs.size(), 4);

        AttrConfig attrConfig5 = new AttrConfig();
        attrConfig5.setAttrName("Attr5");
        attrConfig5.setAttrProps(new HashMap<>());
        attrConfigEntityMgr.save(tenantName, BusinessEntity.Contact, Collections.singletonList(attrConfig5));
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllInEntitiesInReader(tenantName,
                Arrays.asList(BusinessEntity.Contact, BusinessEntity.Account));
        Assert.assertEquals(attrConfigs.size(), 5);

        attrConfigEntityMgr.deleteByAttrNameStartingWith("Attr");
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isNotEmpty(attrConfigs));
        Assert.assertEquals(attrConfigs.size(), 1);

        attrConfigEntityMgr.deleteAllForEntity(tenantName, entity);
        Thread.sleep(500); // wait for replication lag
        attrConfigs = attrConfigEntityMgr.findAllForEntity(tenantName, entity);
        Assert.assertTrue(CollectionUtils.isEmpty(attrConfigs));
    }

    @Test(groups = "functional")
    public void testCleanupTenant() throws InterruptedException {
        tenant1 = TestFrameworkUtils.generateTenantName();
        Thread.sleep(10);
        tenant2 = TestFrameworkUtils.generateTenantName();
        BusinessEntity entity = BusinessEntity.Account;

        AttrConfig attrConfig1 = new AttrConfig();
        attrConfig1.setAttrName("Attr1");
        attrConfig1.setAttrProps(new HashMap<>());
        AttrConfig attrConfig2 = new AttrConfig();
        attrConfig2.setAttrName("Attr2");
        attrConfig2.setAttrProps(new HashMap<>());
        AttrConfig attrConfig3 = new AttrConfig();
        attrConfig3.setAttrName("Attr3");
        attrConfig3.setAttrProps(new HashMap<>());

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

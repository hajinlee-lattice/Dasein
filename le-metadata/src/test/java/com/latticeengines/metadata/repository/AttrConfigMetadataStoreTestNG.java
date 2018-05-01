package com.latticeengines.metadata.repository;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import com.latticeengines.metadata.mds.repository.AttrConfigRepository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AttrConfigMetadataStoreTestNG extends MetadataFunctionalTestNGBase {

    @Inject
    private AttrConfigRepository attrConfigMetadataStore;

    private String tenantId = TestFrameworkUtils.generateTenantName();
    private String uuid;

    @BeforeClass(groups = "functional")
    public void beforeClass() throws InterruptedException {
        saveAttrConfig();
        Thread.sleep(500); // wait for replication lag
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        deleteAttrConfig();
    }

    @Test(groups = "functional")
    public void testFindByNamespace() {
        List<AttrConfigEntity> attrConfigEntities = readAttrConfig();
        AttrConfigEntity attrConfigEntity = attrConfigEntities.get(0);
        Assert.assertNotNull(attrConfigEntity);
        Assert.assertEquals(attrConfigEntity.getTenantId(), tenantId);
        Assert.assertEquals(attrConfigEntity.getEntity(), BusinessEntity.Account);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void saveAttrConfig() {
        uuid = UUID.randomUUID().toString();
        AttrConfigEntity attrConfigEntity = new AttrConfigEntity();
        attrConfigEntity.setUuid(uuid);
        attrConfigEntity.setTenantId(tenantId);
        attrConfigEntity.setEntity(BusinessEntity.Account);
        attrConfigMetadataStore.save(attrConfigEntity);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AttrConfigEntity> readAttrConfig() {
        return attrConfigMetadataStore //
                .findByNamespace(AttrConfigEntity.class, null, tenantId, BusinessEntity.Account);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteAttrConfig() {
        attrConfigMetadataStore.deleteById(uuid);
    }

}

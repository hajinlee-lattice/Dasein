package com.latticeengines.ulysses.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.EnrichedAccount;
import com.latticeengines.ulysses.entitymgr.EnrichedAccountEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;

public class EnrichedAccountEntityMgrImplTestNG extends UlyssesTestNGBase {

    private EnrichedAccountEntityMgr enrichedAccountEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {
        enrichedAccountEntityMgr = new EnrichedAccountEntityMgrImpl(messageService, dataService);
        super.createTable(enrichedAccountEntityMgr.getRepository(), enrichedAccountEntityMgr.getRecordType());
    }

    @Test(groups = "functional")
    public void init() {
        enrichedAccountEntityMgr.init();
    }

    @Test(groups = "functional", dependsOnMethods = { "init" })
    public void create() {
        EnrichedAccount record = new EnrichedAccount();
        record.setId("12345");
        record.setLatticeAccountId("12345");
        record.setTenantId("A.A.Production");
        record.setSourceAccountId("asdfghj");
        record.setAttribute("ExternalId", "abcde");
        enrichedAccountEntityMgr.create(record);
    }

    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findByKey() {
        EnrichedAccount record = enrichedAccountEntityMgr.findByKey("12345");
        assertEquals(record.getId(), "12345");
        assertEquals(record.getLatticeAccountId(), "12345");
        assertEquals(record.getTenantId(), "A.A.Production");
        assertEquals(record.getSourceAccountId(), "asdfghj");
        assertEquals(record.getAttributes().get("ExternalId"), "abcde");
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findAttributesByKey() {
        Map<String, Object> map = enrichedAccountEntityMgr.findAttributesByKey("12345");
        assertEquals(map.get("latticeAccountId"), "12345");
    }
}

package com.latticeengines.ulysses.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.ScoreAndEnrichmentRecord;
import com.latticeengines.ulysses.entitymgr.ScoreAndEnrichmentEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;

public class ScoreAndEnrichmentEntityMgrImplTestNG extends UlyssesTestNGBase {
    
    private ScoreAndEnrichmentEntityMgr scoreAndEnrichmentEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup() {
        scoreAndEnrichmentEntityMgr = new ScoreAndEnrichmentEntityMgrImpl(messageService, dataService);
        super.createTable(scoreAndEnrichmentEntityMgr.getRepository(), scoreAndEnrichmentEntityMgr.getRecordType());
    }
    
    @Test(groups = "functional")
    public void init() {
        scoreAndEnrichmentEntityMgr.init();
    }
    
    @Test(groups = "functional", dependsOnMethods = { "init" })
    public void create() {
        ScoreAndEnrichmentRecord record = new ScoreAndEnrichmentRecord();
        record.setId("12345");
        record.setTenantId("A.A.Production");
        record.setExternalId("asdfghj");
        record.setRequestTimestamp(123456L);
        record.setScore(20.0);
        record.setValue("ExternalId", "abcde");
        record.setCampaignIds(Arrays.asList(new String[] { "xyz", "def" }));
        scoreAndEnrichmentEntityMgr.create(record);
    }

    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findByKey() {
        ScoreAndEnrichmentRecord record = scoreAndEnrichmentEntityMgr.findByKey("12345");
        assertEquals(record.getId(), "12345");
        assertEquals(record.getTenantId(), "A.A.Production");
        assertEquals(record.getExternalId(), "asdfghj");
        assertEquals(record.getRequestTimestamp(), 123456L);
        assertEquals(record.getScore(), 20.0);
        assertEquals((String) record.getAttributes().get("ExternalId"), "abcde");
        assertEquals(record.getCampaignIds().size(), 2);
    }
    
    @SuppressWarnings("rawtypes")
    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findAttributesByKey() {
        Map<String, Object> map = scoreAndEnrichmentEntityMgr.findAttributesByKey("12345");
        assertEquals(((List) map.get("campaignIds")).size(), 2);
    }
}

package com.latticeengines.ulysses.entitymgr.impl;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.domain.exposed.ulysses.Insight;
import com.latticeengines.domain.exposed.ulysses.InsightSection;
import com.latticeengines.domain.exposed.ulysses.InsightSourceType;
import com.latticeengines.ulysses.entitymgr.CampaignEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesTestNGBase;

public class CampaignEntityMgrImplTestNG extends UlyssesTestNGBase {
    
    private CampaignEntityMgr campaignEntityMgr;
    
    private Campaign campaign;
    
    @BeforeClass(groups = "functional")
    public void setup() {
        campaignEntityMgr = new CampaignEntityMgrImpl(messageService, dataService);
        super.createTable(campaignEntityMgr.getRepository(), campaignEntityMgr.getRecordType());
    }
    
    @Test(groups = "functional")
    public void init() {
        campaignEntityMgr.init();
    }
    
    @Test(groups = "functional", dependsOnMethods = { "init" })
    public void create() {
        InsightSection s1 = new InsightSection();
        s1.setHeadline("Headline text");
        s1.setTip("Tip text");
        s1.setDescription("Description text");
        s1.setAttributes((Arrays.asList(new String[] { "A", "B" })));
        s1.setInsightSourceType(InsightSourceType.BOTH);
        
        Insight i1 = new Insight();
        i1.setId("Insight1");
        i1.setName("Insight1");
        i1.setInsightSections(Arrays.asList(new InsightSection[] { s1 }));
        
        campaign = new Campaign();
        campaign.setName("Campaign1");
        campaign.setInsights(Arrays.asList(new Insight[] { i1 }));
        
        campaignEntityMgr.create(campaign);
    }

    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findByKey() {
        Campaign c = campaignEntityMgr.findByKey(campaign.getId());
        assertEquals(c.getName(), campaign.getName());
        assertEquals(c.getInsights().size(), campaign.getInsights().size());
        InsightSection s1 = getFirstSection(c);
        InsightSection expected = getFirstSection(campaign);
        
        assertEquals(s1.getDescription(), expected.getDescription());
        assertEquals(s1.getHeadline(), expected.getHeadline());
        assertEquals(s1.getTip(), expected.getTip());
    }
    
    private InsightSection getFirstSection(Campaign c) {
        return c.getInsights().get(0).getInsightSections().get(0);
    }
    
    @Test(groups = "functional", dependsOnMethods = { "create" })
    public void findAttributesByKey() {
    }
}

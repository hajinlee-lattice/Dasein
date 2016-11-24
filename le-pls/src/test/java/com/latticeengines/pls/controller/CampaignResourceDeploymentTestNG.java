package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.pls.entitymanager.CampaignEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CampaignResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    
    private static final String TABLENAME = "TEST_TABLE";
    
    @Autowired
    private MetadataProxy metadataProxy;
    
    @Autowired
    private CampaignEntityMgr campaignEntityMgr;
    
    private ModelSummary summary; 
    
    
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        switchToSuperAdmin();
        createTable();
    }
    
    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        campaignEntityMgr.deleteAll();
    }

    private void createTable() {
        Table table = new Table();
        table.setName(TABLENAME);
        table.setDisplayName(TABLENAME);
        metadataProxy.deleteTable(mainTestTenant.getId(), table.getName());
        metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
    }

    @Test(groups = "deployment")
    public void createFromModel() throws Exception {
        summary = getDetails(mainTestTenant, "marketo");
        summary.setEventTableName(TABLENAME);
        summary = restTemplate.postForObject(getRestAPIHostPort() + "/pls/modelsummaries", summary, ModelSummary.class);
        List<String> modelIds = Arrays.asList(summary.getId());
        SimpleBooleanResponse response = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/campaigns/CAMPAIGN1/models/", modelIds, SimpleBooleanResponse.class);
        assertNull(response.getErrors());
    }

    @Test(groups = "deployment", dependsOnMethods = { "createFromModel" })
    public void findCampaignByName() throws Exception {
        Campaign campaign = restTemplate.getForObject(getRestAPIHostPort() + "/pls/campaigns/CAMPAIGN1", // 
                Campaign.class, new HashMap<>());
        assertNotNull(campaign);
        assertEquals(campaign.getSegments().size(), 1);
        assertEquals(campaign.getSegments().get(0), summary.getId());
    }

}

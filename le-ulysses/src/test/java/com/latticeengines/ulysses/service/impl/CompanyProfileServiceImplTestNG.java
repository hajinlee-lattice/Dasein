package com.latticeengines.ulysses.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.ulysses.Campaign;
import com.latticeengines.domain.exposed.ulysses.CompanyProfile;
import com.latticeengines.domain.exposed.ulysses.InsightSection;
import com.latticeengines.ulysses.entitymgr.CampaignEntityMgr;
import com.latticeengines.ulysses.functionalframework.UlyssesFunctionalTestNGBase;
import com.latticeengines.ulysses.service.CompanyProfileService;

public class CompanyProfileServiceImplTestNG extends UlyssesFunctionalTestNGBase {
    
    private static final CustomerSpace TESTCUSTOMER = CustomerSpace.parse("UlyssesTest.UlyssesTest.Production");
    
    @Autowired
    private CompanyProfileService companyProfileService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        CampaignEntityMgr campaignEntityMgr = ((CompanyProfileServiceImpl) companyProfileService).getCampaignEntityMgr();
        super.createTable(campaignEntityMgr.getRepository(), campaignEntityMgr.getRecordType());
        
        companyProfileService.setupCampaignForCompanyProfile(TESTCUSTOMER);

        Campaign profile = campaignEntityMgr.findByKey(TESTCUSTOMER + "|PROFILE");
        profile.setTenant(new Tenant(TESTCUSTOMER.toString()));
        InsightSection section = profile.getInsights().get(0).getInsightSections().get(0);
        
        section.setAttributes(Arrays.asList("LE_INDUSTRY", "LE_REVENUE_RANGE", "AdvertisingTechnologiesTopAttributes"));
        section.setHeadline("Some headline");
        section.setTip("Some tip");
        
        profile.getInsights().get(0).setInsightSections(Arrays.asList(new InsightSection[] { section }));
        
        campaignEntityMgr.update(profile);
    }

    @Test(groups = "functional")
    public void getProfile() throws Exception {
        Map<MatchKey, String> matchRequest = new HashMap<>();
        matchRequest.put(MatchKey.Email, "bnguyen@lattice-engines.com");
        CompanyProfile profile = companyProfileService.getProfile(TESTCUSTOMER, matchRequest);
        
        assertEquals(profile.attributes.size(), 3);
    }
}

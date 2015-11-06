package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class ProspectDiscoveryOptionEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    ProspectDiscoveryOptionEntityMgr prospectDiscoveryOptionEntityMgr;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        PROSPECT_DISCOVERY_OPTION_1.setOption(OPTION_1.toString());
        PROSPECT_DISCOVERY_OPTION_1.setValue(STRING_VALUE);
        
        setupUsers();
        cleanupProspectDiscoveryOptionDB();
    }
    
    @Test(groups = { "functional" })
    public void updateOption_calledForNonExistingOption_assertOptionIsCreated() {
        setupSecurityContext(mainTestingTenant);
        assertNull(this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString()));
        
        this.prospectDiscoveryOptionEntityMgr.updateProspectDiscoveryOption(OPTION_1.toString(), STRING_VALUE);
        
        ProspectDiscoveryOption option = this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString());
        assertNotNull(option);
        assertEquals(option.getOption(), OPTION_1.toString());
        assertEquals(option.getValue(), STRING_VALUE);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "updateOption_calledForNonExistingOption_assertOptionIsCreated" })
    public void updateOption_calledForExistingOption_assertOptionValueIsUpdated() {
        setupSecurityContext(mainTestingTenant);
        assertNotNull(this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString()));
        
        this.prospectDiscoveryOptionEntityMgr.updateProspectDiscoveryOption(OPTION_1.toString(), STRING_VALUE_1);
        
        ProspectDiscoveryOption option = this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString());
        assertEquals(option.getValue(), STRING_VALUE_1);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "updateOption_calledForExistingOption_assertOptionValueIsUpdated" })
    public void findOption_calledFromAnotherTenant_assertOptionCannotBeFound() {
        setupSecurityContext(ALTERNATIVE_TESTING_TENANT);
        
        ProspectDiscoveryOption option = this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString());
        
        assertNull(option);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "findOption_calledFromAnotherTenant_assertOptionCannotBeFound" })
    public void deleteOption_calledForExistingOption_assertOptionIsDeleted() {
        setupSecurityContext(mainTestingTenant);
        assertNotNull(this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString()));
    
        this.prospectDiscoveryOptionEntityMgr.deleteProspectDiscoveryOption(OPTION_1.toString());

        assertNull(this.prospectDiscoveryOptionEntityMgr.findProspectDiscoveryOption(OPTION_1.toString()));
    }

}

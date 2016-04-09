package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;

public class ProspectDiscoveryConfigurationResourceTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL = "pls/prospectdiscoveryconfigs/";
    
    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        PROSPECT_DISCOVERY_OPTION_1.setOption(OPTION_1.toString());
        PROSPECT_DISCOVERY_OPTION_1.setValue(STRING_VALUE);
        PROSPECT_DISCOVERY_OPTION_2.setOption(OPTION_2.toString());
        PROSPECT_DISCOVERY_OPTION_2.setValue(DOUBLE_VALUE);
        
        setupUsers();
        cleanupProspectDiscoveryOptionDB();
    }
    
    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        switchToSuperAdmin();
    }
    
    @Test(groups = { "functional" })
    public void postProspectDiscoveryConfiguration_calledWithParams_assertConfigurationIsPosted() {
        restTemplate.put(getRestAPIHostPort() + PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL + OPTION_1.toString(), STRING_VALUE);
        restTemplate.put(getRestAPIHostPort() + PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL + OPTION_2.toString(), DOUBLE_VALUE);
        
        ProspectDiscoveryConfiguration configuration = restTemplate.getForObject(getRestAPIHostPort() + PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL, ProspectDiscoveryConfiguration.class );
        assertNotNull(configuration);
        assertEquals(configuration.getString(OPTION_1, "SHOULD NOT RETURN ME"), STRING_VALUE);
        assertEquals(configuration.getDouble(OPTION_2, 0), Double.parseDouble(DOUBLE_VALUE));
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "postProspectDiscoveryConfiguration_calledWithParams_assertConfigurationIsPosted" })
    public void updateProspectDiscoveryConfiguration_calledWithParams_assertConfigurationIsUpdated() {
        restTemplate.put(getRestAPIHostPort() + PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL + OPTION_1.toString(), STRING_VALUE_1);

        ProspectDiscoveryConfiguration configuration = restTemplate.getForObject(getRestAPIHostPort() + PLS_PROSPECT_DISCOVERY_CONFIGURATION_URL, ProspectDiscoveryConfiguration.class );
        assertNotNull(configuration);
        assertEquals(configuration.getString(OPTION_1, "SHOULD NOT RETURN ME"), STRING_VALUE_1);
    }
    
}

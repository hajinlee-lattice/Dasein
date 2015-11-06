package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;

public interface ProspectDiscoveryConfigurationService {

    ProspectDiscoveryOption findProspectDiscoveryOption(String option);
    
    List<ProspectDiscoveryOption> findAllProspectDiscoveryOptions();
    
    void updateValueForOption(String option, String value);
    
}

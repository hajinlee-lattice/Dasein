package com.latticeengines.pls.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.pls.service.ProspectDiscoveryConfigurationService;

@Component("prospectDiscoveryConfigurationService")
public class ProspectDiscoveryConfigurationServiceImpl implements ProspectDiscoveryConfigurationService {

    @Autowired
    private ProspectDiscoveryOptionEntityMgr prospectDiscovryOptionEntityMgr;

    @Override
    public ProspectDiscoveryOption findProspectDiscoveryOption(String option) {
       return this.prospectDiscovryOptionEntityMgr.findProspectDiscoveryOption(option);
    }

    @Override
    public List<ProspectDiscoveryOption> findAllProspectDiscoveryOptions() {
        return this.prospectDiscovryOptionEntityMgr.findAllProspectDiscoveryOptions();
    }

    @Override
    public void updateValueForOption(String option, String value) {
        this.prospectDiscovryOptionEntityMgr.updateProspectDiscoveryOption(option, value);
    }
    
}

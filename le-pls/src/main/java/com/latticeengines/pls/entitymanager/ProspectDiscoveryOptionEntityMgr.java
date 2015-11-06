package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;

public interface ProspectDiscoveryOptionEntityMgr extends BaseEntityMgr<ProspectDiscoveryOption> { 

    List<ProspectDiscoveryOption> findAllProspectDiscoveryOptions();
    
    ProspectDiscoveryOption findProspectDiscoveryOption(String option);
    
    void deleteProspectDiscoveryOption(String option);
    
    void updateProspectDiscoveryOption(String option, String value);
    
}

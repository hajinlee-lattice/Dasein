package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;

public interface ProspectDiscoveryOptionDao extends BaseDao<ProspectDiscoveryOption> {

    ProspectDiscoveryOption findProspectDiscoveryOption(String option);
    
}

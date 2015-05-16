package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.CrmConfig;

public interface CrmConfigService {

    void config(String crmType, String tenantId, CrmConfig crmConfig);

}

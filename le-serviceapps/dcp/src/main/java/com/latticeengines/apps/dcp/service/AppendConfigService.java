package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;

public interface AppendConfigService {

    DataBlockEntitlementContainer getEntitlement(String customerSpace);

    DplusAppendConfig getAppendConfig(String customerSpace, String sourceId);

}

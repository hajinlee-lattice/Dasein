package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;

public interface AppendConfigService {

    DataBlockEntitlementContainer getEntitlement(String customerSpace);

    DplusAppendConfig getAppendConfig(String customerSpace, String sourceId);

    boolean checkEntitledWith(String customerSpace, DataDomain dataDomain, DataRecordType dataRecordType,
                              String blockName);

}

package com.latticeengines.proxy.exposed.dcp;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;

public interface AppendConfigProxy {

    DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType);

}

package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;

public interface EntitlementService {

    DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType);

    boolean checkEntitledWith(String customerSpace, DataDomain dataDomain, DataRecordType dataRecordType,
                              String blockName);

}

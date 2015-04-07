package com.latticeengines.admin.entitymgr;

import java.util.AbstractMap;
import java.util.List;

import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantEntityMgr {

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);
}

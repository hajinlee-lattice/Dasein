package com.latticeengines.auth.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantConfig;

public interface GlobalAuthUserTenantConfigEntityMgr extends BaseEntityMgrRepository<GlobalAuthUserTenantConfig, Long> {

    List<GlobalAuthUserTenantConfig> findByUserIdAndTenantId(Long userId, Long tenantId);
    
    List<GlobalAuthUserTenantConfig> findByUserId(Long userId);

	List<GlobalAuthUserConfigSummary> findUserConfigSummaryByUserId(Long userId);
	
	GlobalAuthUserConfigSummary findUserConfigSummaryByUserIdTenantId(Long userId, Long tenantId);

}

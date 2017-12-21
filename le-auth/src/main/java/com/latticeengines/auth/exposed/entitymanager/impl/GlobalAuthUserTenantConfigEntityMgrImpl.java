package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantConfigDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantConfigEntityMgr;
import com.latticeengines.auth.exposed.repository.GlobalAuthUserTenantConfigRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserConfigSummary;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantConfig;

@Component("globalAuthUserTenantConfigEntityMgr")
public class GlobalAuthUserTenantConfigEntityMgrImpl extends
        BaseEntityMgrRepositoryImpl<GlobalAuthUserTenantConfig, Long> implements GlobalAuthUserTenantConfigEntityMgr {

    @Autowired
    private GlobalAuthUserTenantConfigDao gaUserTenantConfigDao;
    
    @Autowired
    private GlobalAuthUserTenantConfigRepository gaUserTenantConfigRepository;
    

    @Override
    public BaseDao<GlobalAuthUserTenantConfig> getDao() {
        return gaUserTenantConfigDao;
    }
    
    @Override
    public BaseJpaRepository<GlobalAuthUserTenantConfig, Long> getRepository() {
        return (BaseJpaRepository<GlobalAuthUserTenantConfig, Long>) gaUserTenantConfigRepository;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthUserTenantConfig> findByUserIdAndTenantId(Long userId, Long tenantId) {
        return gaUserTenantConfigRepository.findByGlobalAuthUserPidAndGlobalAuthTenantPid(userId, tenantId);
    }

    @Override
    public List<GlobalAuthUserTenantConfig> findByUserId(Long userId) {
        return gaUserTenantConfigRepository.findByGlobalAuthUserPid(userId);
    }

    @Override
    public List<GlobalAuthUserConfigSummary> findUserConfigSummaryByUserId(Long userId) {
        return gaUserTenantConfigRepository.findGlobalAuthUserConfigSummaryByUserId(userId);
    }

    @Override
    public GlobalAuthUserConfigSummary findUserConfigSummaryByUserIdTenantId(Long userId, Long tenantId) {
    	List<GlobalAuthUserConfigSummary> configSummary = gaUserTenantConfigRepository.findGlobalAuthUserConfigSummaryByUserIdTenantId(userId, tenantId);

    	return configSummary.size() > 0 ? configSummary.get(0) : null;
    }

}

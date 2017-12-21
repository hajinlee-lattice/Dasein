package com.latticeengines.auth.exposed.dao.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantConfigDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantConfig;

@Component("globalAuthUserTenantConfigDao")
public class GlobalAuthUserTenantConfigDaoImpl extends BaseDaoImpl<GlobalAuthUserTenantConfig>
        implements GlobalAuthUserTenantConfigDao {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthUserTenantConfigDaoImpl.class);

    @Override
    protected Class<GlobalAuthUserTenantConfig> getEntityClass() {
        return GlobalAuthUserTenantConfig.class;
    }
    
}

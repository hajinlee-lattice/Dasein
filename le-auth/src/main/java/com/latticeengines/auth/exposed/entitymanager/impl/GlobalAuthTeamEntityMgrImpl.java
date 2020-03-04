package com.latticeengines.auth.exposed.entitymanager.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.auth.exposed.dao.GlobalAuthTeamDao;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTeamEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

@Component("globalAuthTeamEntityMgr")
public class GlobalAuthTeamEntityMgrImpl extends BaseEntityMgrImpl<GlobalAuthTeam>
        implements GlobalAuthTeamEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthTeamEntityMgrImpl.class);

    @Inject
    private GlobalAuthTeamDao globalAuthTeamDao;

    @Override
    public BaseDao<GlobalAuthTeam> getDao() {
        return globalAuthTeamDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(GlobalAuthTeam globalAuthTeam) {
        Date now = new Date(System.currentTimeMillis());
        globalAuthTeam.setCreationDate(now);
        globalAuthTeam.setLastModificationDate(now);
        getDao().create(globalAuthTeam);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void update(GlobalAuthTeam globalAuthTeam) {
        globalAuthTeam.setLastModificationDate(new Date(System.currentTimeMillis()));
        getDao().update(globalAuthTeam);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(GlobalAuthTeam globalAuthTeam) {
        super.delete(globalAuthTeam);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<GlobalAuthTeam> findByTenantId(Long tenantId) {
        return getDao().findAllByField("Tenant_ID", tenantId);
    }

}

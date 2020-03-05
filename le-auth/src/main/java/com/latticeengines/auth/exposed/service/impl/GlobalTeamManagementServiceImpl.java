package com.latticeengines.auth.exposed.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTeamEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.auth.exposed.service.GlobalTeamManagementService;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

@Component("globalTeamManagementService")
public class GlobalTeamManagementServiceImpl implements
        GlobalTeamManagementService {

    private static final Logger log = LoggerFactory.getLogger(GlobalTeamManagementServiceImpl.class);

    @Inject
    private GlobalAuthTeamEntityMgr globalAuthTeamEntityMgr;

    @Inject
    private GlobalAuthUserTenantRightEntityMgr globalAuthUserTenantRightEntityMgr;

    @Override
    public void createGlobalAuthTeam(GlobalAuthTeam globalAuthTeam) {
        globalAuthTeamEntityMgr.create(globalAuthTeam);
    }

    @Override
    public void updateGlobalAuthTeam(GlobalAuthTeam globalAuthTeam) {
        globalAuthTeamEntityMgr.update(globalAuthTeam);
    }


}

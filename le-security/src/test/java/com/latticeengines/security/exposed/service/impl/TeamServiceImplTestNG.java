package com.latticeengines.security.exposed.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.globalauth.GlobalTenantManagementService;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class TeamServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Inject
    GlobalTenantManagementService globalTenantManagementService;

    @Inject
    UserService userService;

    @Inject
    TeamService teamService;

    @Inject
    TenantEntityMgr tenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() {

    }

    @Test(groups = "functional")
    public void testGrantAndRevokeAccessLevel() {
        Tenant tenant = tenantEntityMgr.findByTenantPid(5L);
        MultiTenantContext.setTenant(tenant);
        List<GlobalTeam> globalTeams = teamService.getTeamsByUserName("test2@lattice-engines.com");
    }

}

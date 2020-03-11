package com.latticeengines.security.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class TeamServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Inject
    private TeamService teamService;

    private Tenant tenant;
    private Tenant anotherTenant;

    private String username1InTenant = "teamTestUsername1@lattice-engines.com";
    private String username2InTenant = "teamTestUsername2@domain.com";
    private String usernameInAnotherTenant = "teamTestUsername3@lattice-engines.com";

    private String teamName1InTenant = "teamTestName1";
    private String teamName2InTenant = "teamTestName2";
    private String teamNameInAnotherTenant = "teamTestName3";

    @BeforeClass(groups = "functional")
    public void setup() {
        tenant = createTenant(CustomerSpace.parse("TeamServiceTestTenant").toString(), "TeamServiceTestTenant");
        anotherTenant = createTenant(CustomerSpace.parse("TeamServiceTestAnotherTenant").toString(), "TeamServiceTestAnotherTenant");
        createUser(username1InTenant);
        createUser(username2InTenant);
        createUser(usernameInAnotherTenant);

        globalUserManagementService.grantRight("INTERNAL_ADMIN", tenant.getId(), username1InTenant);
        globalUserManagementService.grantRight("EXTERNAL_ADMIN", tenant.getId(), username2InTenant);
        globalUserManagementService.grantRight("INTERNAL_ADMIN", anotherTenant.getId(), usernameInAnotherTenant);
    }

    private void createUser(String username) {
        globalUserManagementService.deleteUser(username);
        String firstLastName = username.substring(0, username1InTenant.indexOf("@"));
        createUser(username, username, firstLastName, firstLastName);
    }

    @AfterClass(groups = {"functional"})
    public void tearDown() {
        makeSureUserDoesNotExist(username1InTenant);
        makeSureUserDoesNotExist(username2InTenant);
        makeSureUserDoesNotExist(usernameInAnotherTenant);
        deleteTenant(tenant);
        deleteTenant(anotherTenant);
    }

    private void validateTeamInfo(GlobalTeam globalTeam, String teamName, String teamMemberName) {
        assertNotNull(globalTeam.getTeamId());
        assertEquals(globalTeam.getTeamName(), teamName);
        assertNotNull(globalTeam.getCreatedByUser());
        assertEquals(globalTeam.getTeamMembers().size(), 1);
        assertEquals(globalTeam.getTeamMembers().get(0).getEmail(), teamMemberName);
    }

    @Test(groups = "functional")
    public void testCurdTeam() {
        MultiTenantContext.setTenant(tenant);
        String teamId = teamService.createTeam(username1InTenant, getGlobalTeamData(teamName1InTenant, Sets.newHashSet(username1InTenant, username2InTenant)));
        assertNotNull(teamId);
        // create
        teamService.createTeam(username2InTenant, getGlobalTeamData(teamName2InTenant, Sets.newHashSet(username1InTenant)));
        List<GlobalTeam> globalTeams = teamService.getTeams(getUser(username1InTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 2);
        globalTeams = teamService.getTeamsByUserName(username1InTenant, getUser(username1InTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 2);
        globalTeams = teamService.getTeamsByUserName(username2InTenant, getUser(username1InTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 1);
        // edit
        teamService.editTeam(teamId, getGlobalTeamData(teamName1InTenant, Sets.newHashSet(username1InTenant)));
        globalTeams = teamService.getTeamsByUserName(username2InTenant, getUser(username1InTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 0);
        // delete
        teamService.deleteTeam(teamId);
        globalTeams = teamService.getTeams(getUser(username1InTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 1);
        teamService.deleteTeamByTenantId();
        MultiTenantContext.setTenant(anotherTenant);
        teamService.createTeam(usernameInAnotherTenant, getGlobalTeamData(teamNameInAnotherTenant, Sets.newHashSet(usernameInAnotherTenant)));
        globalTeams = teamService.getTeamsByUserName(usernameInAnotherTenant, getUser(usernameInAnotherTenant, AccessLevel.INTERNAL_ADMIN.name()));
        assertEquals(globalTeams.size(), 1);
        validateTeamInfo(globalTeams.get(0), teamNameInAnotherTenant, usernameInAnotherTenant);
        teamService.deleteTeamByTenantId();
    }
}

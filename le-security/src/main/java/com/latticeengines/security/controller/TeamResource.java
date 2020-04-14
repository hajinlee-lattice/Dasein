package com.latticeengines.security.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.TeamService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Team Management")
@RestController
@RequestMapping("/teams")
public class TeamResource {

    private static final Logger log = LoggerFactory.getLogger(TeamResource.class);

    @Inject
    private TeamService teamService;

    @GetMapping(value = "/username/{username:.+}")
    @ResponseBody
    @ApiOperation(value = "Get teams by username")
    public List<GlobalTeam> getTeamsByUsername(@PathVariable(value = "username") String username,
                                               @RequestParam(value = "withTeamMember", required = false, defaultValue = "true") boolean withTeamMember) {
        User loginUser = MultiTenantContext.getUser();
        return teamService.getTeamsByUserName(username, loginUser, withTeamMember);
    }

    @GetMapping(value = "/session")
    @ResponseBody
    @ApiOperation(value = "Get teams by username")
    public List<GlobalTeam> getTeamsFromSession(
            @RequestParam(value = "withTeamMember", required = false, defaultValue = "true") boolean withTeamMember,
            @RequestParam(value = "appendDefaultGlobalTeam", required = false, defaultValue = "true") boolean appendDefaultGlobalTeam) {
        List<GlobalTeam> globalTeams = teamService.getTeamsFromSession(withTeamMember, appendDefaultGlobalTeam);
        return globalTeams;
    }

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "List all teams")
    public List<GlobalTeam> getAllTeams() {
        return teamService.getTeamsInContext(true, false);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Create a new team")
    @PreAuthorize("hasRole('Edit_PLS_Teams')")
    public String createTeam(@RequestBody GlobalTeamData globalTeamData) {
        return teamService.createTeam(MultiTenantContext.getUser().getEmail(), globalTeamData);
    }

    @PutMapping(value = "/teamId/{teamId}")
    @ResponseBody
    @ApiOperation(value = "Update a team")
    @PreAuthorize("hasRole('Edit_PLS_Teams')")
    public Boolean editTeam(@PathVariable("teamId") String teamId, //
                            @RequestBody GlobalTeamData globalTeamData) {
        log.info("Edit team {}.", teamId);
        return teamService.editTeam(teamId, globalTeamData);
    }

    @DeleteMapping(value = "/teamId/{teamId}")
    @ResponseBody
    @ApiOperation(value = "Delete a team")
    @PreAuthorize("hasRole('Edit_PLS_Teams')")
    public Boolean deleteTeam(@PathVariable("teamId") String teamId) {
        log.info("Delete team " + teamId);
        return true;

    }

    @GetMapping(value = "/{teamId}/dependencies")
    @ResponseBody
    @ApiOperation(value = "Get all the dependencies")
    public Map<String, List<String>> getDependencies(@PathVariable String teamId) throws Exception {
        log.info(String.format("get all dependencies for teamId=%s", teamId));
        return teamService.getDependencies(teamId);
    }
}

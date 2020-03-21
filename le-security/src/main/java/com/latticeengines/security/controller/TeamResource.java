package com.latticeengines.security.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

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
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.GlobalTeamData;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.service.TeamService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Team Management")
@RestController
@RequestMapping("/teams")
public class TeamResource {

    private static final Logger log = LoggerFactory.getLogger(TeamResource.class);

    @Inject
    private TeamService teamService;

    @Inject
    private SessionService sessionService;

    @Inject
    private UserService userService;

    @GetMapping(value = "/username/{username}")
    @ResponseBody
    @ApiOperation(value = "Get teams by username")
    public List<GlobalTeam> getTeamsByUsername(HttpServletRequest request,
                                               @PathVariable(value = "username") String username) {
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        return teamService.getTeamsByUserName(username, loginUser);
    }

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "List all teams")
    public List<GlobalTeam> getAllTeams(HttpServletRequest request) {
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        return teamService.getTeams(loginUser);
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Create a new team")
    @PreAuthorize("hasRole('Edit_PLS_Teams')")
    public String createTeam(@RequestBody GlobalTeamData globalTeamData, HttpServletRequest request) {
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        return teamService.createTeam(loginUser.getUsername(), globalTeamData);
    }

    @PutMapping(value = "/teamId/{teamId}")
    @ResponseBody
    @ApiOperation(value = "Update a team")
    public Boolean editTeam(@PathVariable("teamId") String teamId, //
                            @RequestBody GlobalTeamData globalTeamData, HttpServletRequest request) {
        log.info("Edit team {}.", teamId);
        User loginUser = SecurityUtils.getUserFromRequest(request, sessionService, userService);
        checkUser(loginUser);
        return teamService.editTeam(loginUser, teamId, globalTeamData);
    }

    private void checkUser(User user) {
        if (user == null) {
            throw new LedpException(LedpCode.LEDP_18221);
        }
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

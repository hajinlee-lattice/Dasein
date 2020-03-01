package com.latticeengines.pls.controller;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.auth.GlobalTeam;
import com.latticeengines.domain.exposed.auth.UpdateTeamUsersRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "Team Management")
@RestController
@RequestMapping("/teams")
public class TeamResource {
    private static final Logger log = LoggerFactory.getLogger(TeamResource.class);

    private static ObjectMapper jsonParser = new ObjectMapper();

    // For mocking purpose, use camille LockerManager to
    // temporarily store team information
    private static final String LOCK_NAME = "TeamLock";

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "List all teams")
    public String getAllTeams() {
        String teams = readData();
        log.info("Get all teams, teams are: " + teams);
        return teams;
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Create a new team")
    public GlobalTeam createTeam( //
            @RequestParam(value = "teamName", required = true) String teamName, //
            @RequestParam(value = "createdByUser") String createdByUser, //
            @ApiParam(value = "List of user ids to assign to the team", required = false) //
            @RequestParam(value = "teamMembers") Set<String> teamMembers) {

        Preconditions.checkArgument(StringUtils.isNotBlank(teamName), "Team name can't be empty");

        GlobalTeam newTeam = new GlobalTeam();
        newTeam.setTeamId(GlobalTeam.generateId());
        newTeam.setTeamName(teamName);
        newTeam.setCreatedByUser(createdByUser);
        newTeam.setTeamMembers(teamMembers);

        // Read back the current team data first
        String curData = readData();
        log.info(curData);

        // Construct new json data by appending the new team info
        String newData = null;
        ArrayNode node = new ArrayNode(jsonParser.getNodeFactory());
        if (StringUtils.isNotBlank(curData)) {
            try {
                node = (ArrayNode) jsonParser.readTree(curData);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to process team data", e);
            }
        }
        newData = JsonUtils.serialize(node.addPOJO(newTeam));
        if (writeData(newData)) {
            log.info("Created team " + newTeam.getTeamName() + " with id " + newTeam.getTeamId());
            return newTeam;
        } else {
            throw new RuntimeException("Failed to write team data");
        }
    }

    @PutMapping(value = "/{teamId}")
    @ResponseBody
    @ApiOperation(value = "Update a team")
    public Boolean editTeam( //
            @PathVariable("teamId") String teamId, //
            @RequestParam(value = "teamName") String teamName, //
            @ApiParam(value = "List of user ids to assign to the team", required = false) //
            @RequestParam(value = "teamMembers") Set<String> teamMembers) {
        log.info("Edit team " + teamId);
        String teams = readData();
        ArrayNode teamNodes = new ArrayNode(jsonParser.getNodeFactory());
        Set<GlobalTeam> teamSet = new HashSet<>();
        try {
            teamNodes = (ArrayNode) jsonParser.readTree(teams);
        } catch (JsonProcessingException e) {
            log.error("Failed to process team data", e);
            return false;
        }

        for (JsonNode node : teamNodes) {
            GlobalTeam team = JsonUtils.deserialize(node.toString(), GlobalTeam.class);
            if (teamId.equalsIgnoreCase(team.getTeamId())) { // find the team
                if (StringUtils.isNotBlank(teamName)) {
                    team.setTeamName(teamName);
                }
                if (teamMembers != null) {
                    team.setTeamMembers(teamMembers);
                }
            }

            teamSet.add(team);
        }

        return writeData(JsonUtils.serialize(teamSet));
    }

    @DeleteMapping(value = "/{teamId}")
    @ResponseBody
    @ApiOperation(value = "Update a team")
    public Boolean deleteTeam(@PathVariable("teamId") String teamId) {
        log.info("Delete team " + teamId);
        String teams = readData();
        ArrayNode teamNodes = null;
        Set<GlobalTeam> teamSet = new HashSet<>();
        try {
            teamNodes = (ArrayNode) jsonParser.readTree(teams);
            for (JsonNode node : teamNodes) {
                GlobalTeam team = JsonUtils.deserialize(node.toString(), GlobalTeam.class);
                if (!teamId.equalsIgnoreCase(team.getTeamId())) {
                    teamSet.add(JsonUtils.deserialize(node.toString(), GlobalTeam.class));
                }
            }

            return writeData(JsonUtils.serialize(teamSet));
        } catch (JsonProcessingException e) {
            log.error("Failed to process team data", e);
            return false;
        }
    }

    @PutMapping(value = "/{teamId}/users")
    @ResponseBody
    @ApiOperation(value = "Manager user by team")
    public Boolean manageUserByTeam( //
            @PathVariable("teamId") String teamId, //
            @ApiParam(value = " Request to assign or remove users", required = true) //
            @RequestBody UpdateTeamUsersRequest teamUsersRequest) {
        if (teamUsersRequest == null) {
            log.info("Team assignment is not specified...");
            return true;
        }
        log.info("Manage members for team " + teamId);
        String teams = readData();
        ArrayNode teamNodes = null;
        Set<GlobalTeam> teamSet = new HashSet<>();
        try {
            teamNodes = (ArrayNode) jsonParser.readTree(teams);
        } catch (JsonProcessingException e) {
            log.error("Failed to process team data", e);
            return false;
        }

        for (JsonNode node : teamNodes) {
            GlobalTeam team = JsonUtils.deserialize(node.toString(), GlobalTeam.class);
            if (teamId.equalsIgnoreCase(team.getTeamId())) { // find the team
                Set<String> teamMembers = team.getTeamMembers();
                teamMembers.addAll(teamUsersRequest.getUserToAssign());
                teamMembers.removeAll(teamUsersRequest.getUserToRemove());
            }

            teamSet.add(team);
        }

        return writeData(JsonUtils.serialize(teamSet));
    }

    private String readData() {
        String data = null;
        // Read the data from camille lock manager
        LockManager.registerDivisionPrivateLock(LOCK_NAME);
        LockManager.acquireWriteLock(LOCK_NAME, 1, TimeUnit.SECONDS);
        try {
            data = LockManager.peekData(LOCK_NAME, 1, TimeUnit.SECONDS);
            log.info("read data: " + data);
        } catch (Exception e) {
            log.error("Failed to read data", e);
            LockManager.releaseWriteLock(LOCK_NAME);
            return null;
        }
        LockManager.releaseWriteLock(LOCK_NAME);

        return data;
    }

    private boolean writeData(String data) {
        boolean success = false;
        if (StringUtils.isNotBlank(data)) {
            log.info("Write data: " + data);
            try {
                // Write to camille via locker manager
                LockManager.registerDivisionPrivateLock(LOCK_NAME);
                LockManager.acquireWriteLock(LOCK_NAME, 1, TimeUnit.SECONDS);
                LockManager.upsertData(LOCK_NAME, data, CamilleEnvironment.getDivision());
                success = true;
            } catch (Exception e) {
                log.error("Failed to upsert data", e);
            } finally {
                LockManager.releaseWriteLock(LOCK_NAME);
            }
        }

        return success;
    }
}

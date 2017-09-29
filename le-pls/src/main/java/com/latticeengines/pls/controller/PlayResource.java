package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.workflow.PlayLaunchWorkflowSubmitter;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "play", description = "REST resource for play")
@RestController
@RequestMapping("/play")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class PlayResource {

    private static final Logger log = LoggerFactory.getLogger(PlayResource.class);

    @Autowired
    private PlayService playService;

    @Autowired
    private PlayLaunchService playLaunchService;

    @Autowired
    private PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all full plays for a tenant")
    public List<Play> getPlays(HttpServletRequest request, //
            HttpServletResponse response,
            @RequestParam(value = "shouldLoadCoverage", required = false) Boolean shouldLoadCoverage, //
            @RequestParam(value = "ratingEngineId", required = false) String ratingEngineId) {
        // by default shouldLoadCoverage flag should be false otherwise play
        // listing API takes lot of time to load
        shouldLoadCoverage = shouldLoadCoverage == null ? false : shouldLoadCoverage;
        return playService.getAllFullPlays(shouldLoadCoverage, ratingEngineId);
    }

    @RequestMapping(value = "/launches/dashboard", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Play launch dashboard for a tenant")
    public PlayLaunchDashboard getPlayLaunchDashboard(HttpServletRequest request, //
            @ApiParam(value = "List of Play Ids for which to load dashboard info", required = false) @RequestParam(value = "playIds", required = false) List<String> playIds, //
            @ApiParam(value = "List of launch states to consider", required = false) @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) @RequestParam(value = "plays", required = true) long start, //
            @ApiParam(value = "Play launch offset from start time", required = true) @RequestParam(value = "offset", required = true) long offset, //
            @ApiParam(value = "Maximum number of play launches to consider", required = true) @RequestParam(value = "max", required = true) long max) {
        // adding signature
        // TODO - add impl
        return new PlayLaunchDashboard();
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get full play for a specific tenant based on playName")
    public Play getPlay(@PathVariable String playName, HttpServletRequest request, HttpServletResponse response) {
        Play play = playService.getFullPlayByName(playName);
        return play;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register a play")
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    public Play createOrUpdate(@RequestBody Play play, HttpServletRequest request) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Tenant is null for the request");
            return null;
        }
        if (play == null) {
            throw new NullPointerException("Play is null");
        }
        return playService.createOrUpdate(play, tenant.getId());
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a play")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public Boolean delete(@PathVariable String playName) {
        playService.deleteByName(playName);
        return true;
    }

    @RequestMapping(value = "/{playName}/launches", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Create play launch for a given play")
    public PlayLaunch createPlayLaunch( //
            @PathVariable("playName") String playName, //
            @RequestBody PlayLaunch playLaunch, //
            HttpServletResponse response) {
        Play play = playService.getPlayByName(playName);
        validatePlayBeforeLaunch(play);
        validatePlayLaunchBeforeLaunch(playLaunch, play);
        if (play != null) {
            playLaunch.setLaunchState(LaunchState.Launching);
            playLaunch.setPlay(play);
            playLaunchService.create(playLaunch);
            String appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
            playLaunch.setApplicationId(appId);
            playLaunchService.update(playLaunch);
        } else {
            log.error("Invalid playName: " + playName);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        return playLaunch;
    }

    private void validatePlayBeforeLaunch(Play play) {
        if (play.getRatingEngine() == null) {
            throw new LedpException(LedpCode.LEDP_18149, new String[] { play.getName() });
        } else if (play.getRatingEngine().getStatus() != RatingEngineStatus.ACTIVE) {
            throw new LedpException(LedpCode.LEDP_18155, new String[] { play.getName() });
        }

    }

    private void validatePlayLaunchBeforeLaunch(PlayLaunch playLaunch, Play play) {
        if (playLaunch.getBucketsToLaunch() == null) {
            throw new LedpException(LedpCode.LEDP_18156, new String[] { play.getName() });
        }
    }

    @RequestMapping(value = "/{playName}/launches", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of launches for a given play")
    public List<PlayLaunch> getPlayLaunches(@PathVariable("playName") String playName, //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates) {
        Play play = playService.getPlayByName(playName);
        if (launchStates == null) {
            launchStates = new ArrayList<>();
        }
        return playLaunchService.findByPlayId(play.getPid(), launchStates);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get play launch for a given play and launch id")
    public PlayLaunch getPlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        return playLaunchService.findByLaunchId(launchId);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}/{action}", //
            method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch for a given play and launch id with given action")
    public PlayLaunch updatePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @PathVariable("action") LaunchState action) {
        PlayLaunch existingPlayLaunch = playLaunchService.findByLaunchId(launchId);
        if (existingPlayLaunch != null) {
            if (LaunchState.canTransit(existingPlayLaunch.getLaunchState(), action)) {
                existingPlayLaunch.setLaunchState(action);
                return playLaunchService.update(existingPlayLaunch);
            }
        }
        return existingPlayLaunch;
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}", method = RequestMethod.DELETE)
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Delete play launch for a given play and launch id")
    public void deletePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        if (playLaunch != null) {
            playLaunchService.deleteByLaunchId(launchId);
        }
    }
}

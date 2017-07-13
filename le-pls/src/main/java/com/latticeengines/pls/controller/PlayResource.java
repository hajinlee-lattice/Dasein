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

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.pls.workflow.PlayLaunchWorkflowSubmitter;
import com.latticeengines.security.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

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
    @ApiOperation(value = "Get all plays for a tenant")
    public List<Play> getPlays(HttpServletRequest request, HttpServletResponse response) {
        return playService.getAllPlays();
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get play for a specific tenant based on playName")
    public Play getPlay(@PathVariable String playName, HttpServletRequest request, HttpServletResponse response) {
        Play play = playService.getPlayByName(playName);
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
    public PlayLaunch createPlayLaunch(@PathVariable("playName") String playName, //
            @RequestBody PlayLaunch playLaunch) {
        Play play = playService.getPlayByName(playName);
        playLaunch.setPlay(play);
        playLaunchService.create(playLaunch);
        String appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
        playLaunch.setApplicationId(appId);
        playLaunchService.update(playLaunch);
        return playLaunch;
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

        if (action != null && action != LaunchState.Launching) {
            if (existingPlayLaunch != null) {
                if (existingPlayLaunch.getLaunchState() == LaunchState.Launching) {
                    existingPlayLaunch.setLaunchState(action);
                    return playLaunchService.update(existingPlayLaunch);
                }
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

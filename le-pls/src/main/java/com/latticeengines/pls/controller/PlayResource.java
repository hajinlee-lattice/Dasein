package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.http.MediaType;
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

import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.pls.service.PlayService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "play", description = "REST resource for play")
@RestController
@RequestMapping("/play")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class PlayResource {

    @Inject
    private PlayService playService;

    // -----
    // Plays
    // -----
    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get all full plays for a tenant")
    public List<Play> getPlays(@RequestParam(value = "shouldLoadCoverage", required = false) Boolean shouldLoadCoverage, //
            @RequestParam(value = "ratingEngineId", required = false) String ratingEngineId) {
        return playService.getPlays(shouldLoadCoverage, ratingEngineId);
    }

    @GetMapping("/{playName}")
    @ResponseBody
    @ApiOperation(value = "Get full play for a specific tenant based on playName")
    public Play getPlay(@PathVariable String playName) {
        return playService.getPlay(playName);
    }

    @PostMapping(consumes = { KryoHttpMessageConverter.KRYO_VALUE, MediaType.APPLICATION_JSON_VALUE,
            "application/x-kryo;charset=UTF-8" })
    @ResponseBody
    @ApiOperation(value = "Register a play")
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    public Play createOrUpdate(@RequestBody Play play) {
        return playService.createOrUpdate(play);
    }

    @DeleteMapping("/{playName}")
    @ResponseBody
    @ApiOperation(value = "Delete a play")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public Boolean delete(@PathVariable String playName, //
            @RequestParam(value = "hardDelete", required = false, defaultValue = "false") Boolean hardDelete) {
        playService.delete(playName, hardDelete);
        return true;
    }
    // -----
    // Plays
    // -----

    // --------
    // Channels
    // --------

    @GetMapping("/{playName}/channels")
    @ResponseBody
    @ApiOperation(value = "For the given play, get a list of play launch channels")
    public List<PlayLaunchChannel> getPlayLaunchChannels(@PathVariable("playName") String playName, //
            @RequestParam(value = "include-unlaunched-channels", required = false, defaultValue = "false") Boolean includeUnlaunchedChannels) {
        return playService.getPlayLaunchChannels(playName, includeUnlaunchedChannels);
    }

    @PostMapping("/{playName}/channels")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')") // ask later
    @ApiOperation(value = "Create play launch channel for a given play")
    public PlayLaunchChannel createPlayLaunchChannel( //
            @PathVariable("playName") String playName, //
            @RequestBody PlayLaunchChannel playLaunchChannel, //
            @RequestParam(value = "launch-now", required = false, defaultValue = "false") Boolean launchNow) {
        return playService.createPlayLaunchChannel(playName, playLaunchChannel, launchNow);
    }

    @PutMapping("/{playName}/channels/{channelId}")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch channel for a given play")
    public PlayLaunchChannel updatePlayLaunchChannel( //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId, //
            @RequestBody PlayLaunchChannel playLaunchChannel, //
            @RequestParam(value = "launch-now", required = false, defaultValue = "false") Boolean launchNow) {
        return playService.updatePlayLaunchChannel(playName, channelId, playLaunchChannel, launchNow);
    }
    // --------
    // Channels
    // --------

    // --------
    // Launches
    // --------

    @GetMapping("/launches/dashboard")
    @ResponseBody
    @ApiOperation(value = "Play launch dashboard for a tenant")
    public PlayLaunchDashboard getPlayLaunchDashboard(HttpServletRequest request, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays", required = false) //
            @RequestParam(value = "playName", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "orgId", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "externalSysType", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider", required = false) //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "startTimestamp", required = true) Long startTimestamp, //
            @ApiParam(value = "Play launch offset from start time", required = true) //
            @RequestParam(value = "offset", required = true) Long offset, //
            @ApiParam(value = "Maximum number of play launches to consider", required = true) //
            @RequestParam(value = "max", required = true) Long max, //
            @ApiParam(value = "Sort by", required = false) //
            @RequestParam(value = "sortby", required = false) String sortby, //
            @ApiParam(value = "Sort in descending order", required = false, defaultValue = "true") //
            @RequestParam(value = "descending", required = false, defaultValue = "true") boolean descending, //
            @ApiParam(value = "End date in Unix timestamp", required = false) //
            @RequestParam(value = "end-timestamp", required = false) Long endTimestamp) {
        return playService.getPlayLaunchDashboard(playName, orgId,
                externalSysType, launchStates, startTimestamp, offset, max, sortby, descending, endTimestamp);
    }

    @GetMapping("/launches/dashboard/count")
    @ResponseBody
    @ApiOperation(value = "Play entries count for launch dashboard for a tenant")
    public Long getPlayLaunchDashboardEntriesCount(HttpServletRequest request, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays", required = false) //
            @RequestParam(value = "playName", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "orgId", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "externalSysType", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider", required = false) //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "startTimestamp", required = true) Long startTimestamp, //
            @ApiParam(value = "End date in Unix timestamp", required = false) //
            @RequestParam(value = "endTimestamp", required = false) Long endTimestamp) {
        return playService.getPlayLaunchDashboardEntriesCount(playName, orgId, externalSysType, launchStates, startTimestamp, endTimestamp);
    }

    @PostMapping("/{playName}/launches")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Create play launch for a given play")
    @Deprecated
    public PlayLaunch createPlayLaunch( //
            @PathVariable("playName") String playName, @RequestBody PlayLaunch playLaunch) {
        return playService.createPlayLaunch(playName, playLaunch);
    }

    @PostMapping("/{playName}/launches/{launchId}")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch for a given play")
    @Deprecated
    public PlayLaunch updatePlayLaunch( //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestBody PlayLaunch playLaunch) {
        return playService.updatePlayLaunch(playName, launchId, playLaunch);
    }

    @PostMapping("/{playName}/launches/{launchId}/launch")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Launch a given play")
    @Deprecated
    public PlayLaunch launchPlay(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        return playService.launchPlay(playName, launchId);
    }

    @GetMapping("/{playName}/launches")
    @ResponseBody
    @ApiOperation(value = "Get list of launches for a given play")
    public List<PlayLaunch> getPlayLaunches(@PathVariable("playName") String playName, //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates) {
        return playService.getPlayLaunches(playName, launchStates);
    }

    @GetMapping("/{playName}/launches/{launchId}")
    @ResponseBody
    @ApiOperation(value = "Get play launch for a given play and launch id")
    public PlayLaunch getPlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        return playService.getPlayLaunch(playName, launchId);
    }

    @PutMapping("/{playName}/launches/{launchId}/{action}")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch for a given play and launch id with given action")
    public PlayLaunch updatePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @PathVariable("action") LaunchState action) {
        return playService.updatePlayLaunch(playName, launchId, action);
    }

    @DeleteMapping("/{playName}/launches/{launchId}")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Delete play launch for a given play and launch id")
    public void deletePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam(value = "hardDelete", required = false, defaultValue = "false") Boolean hardDelete) {
        playService.deletePlayLaunch(playName, launchId, hardDelete);
    }
    // --------
    // Launches
    // --------

}

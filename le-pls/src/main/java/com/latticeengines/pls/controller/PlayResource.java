package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.converter.KryoHttpMessageConverter;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannelMap;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "play", description = "REST resource for play")
@RestController
@RequestMapping("/play")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class PlayResource {

    private static final Logger log = LoggerFactory.getLogger(PlayResource.class);

    @Inject
    private PlayProxy playProxy;

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
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlays(tenant.getId(), shouldLoadCoverage, ratingEngineId);
    }

    @RequestMapping(value = "/launches/dashboard", method = RequestMethod.GET, headers = "Accept=application/json")
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
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboard(tenant.getId(), playName, launchStates, startTimestamp, offset, max,
                sortby, descending, endTimestamp, orgId, externalSysType);
    }

    @RequestMapping(value = "/launches/dashboard/count", method = RequestMethod.GET, headers = "Accept=application/json")
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
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboardEntriesCount(tenant.getId(), playName, launchStates, startTimestamp,
                endTimestamp, orgId, externalSysType);
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get full play for a specific tenant based on playName")
    public Play getPlay(@PathVariable String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlay(tenant.getId(), playName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, //
            // headers = "Accept=application/json", //
            consumes = { KryoHttpMessageConverter.KRYO_VALUE, MediaType.APPLICATION_JSON_VALUE,
                    "application/x-kryo;charset=UTF-8" })
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

        if (StringUtils.isEmpty(play.getCreatedBy())) {
            play.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(play.getUpdatedBy())) {
            play.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.createOrUpdatePlay(tenant.getId(), play);
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a play")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public Boolean delete(@PathVariable String playName, //
            @RequestParam(value = "hardDelete", required = false, defaultValue = "false") Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlay(tenant.getId(), playName, hardDelete);
        return true;
    }

    @RequestMapping(value = "/{playName}/launches", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Create play launch for a given play")
    public PlayLaunch createPlayLaunch( //
            @PathVariable("playName") String playName, @RequestBody PlayLaunch playLaunch, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunch.getCreatedBy())) {
            playLaunch.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunch.getUpdatedBy())) {
            playLaunch.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.createPlayLaunch(tenant.getId(), playName, playLaunch);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch for a given play")
    public PlayLaunch updatePlayLaunch( //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestBody PlayLaunch playLaunch, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunch.getCreatedBy())) {
            playLaunch.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunch.getUpdatedBy())) {
            playLaunch.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.updatePlayLaunch(tenant.getId(), playName, launchId, playLaunch);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}/launch", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Launch a given play")
    public PlayLaunch launchPlay(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.launchPlay(tenant.getId(), playName, launchId, false);
    }

    @RequestMapping(value = "/{playName}/launches", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of launches for a given play")
    public List<PlayLaunch> getPlayLaunches(@PathVariable("playName") String playName, //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunches(tenant.getId(), playName, launchStates);
    }

    @RequestMapping(value = "/{playName}/channels", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "For the given play, get a map between each system org and their most recent play launch")
    public PlayLaunchChannelMap PlayLaunchChannelMap(@PathVariable("playName") String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchChannelMap(tenant.getId(), playName);
    }

    @RequestMapping(value = "/{playName}/channels", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')") // ask later
    @ApiOperation(value = "Create play launch channel for a given play")
    public PlayLaunchChannel createPlayLaunchChannel( //
            @PathVariable("playName") String playName, @RequestBody PlayLaunchChannel playLaunchChannel, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunchChannel.getCreatedBy())) {
            playLaunchChannel.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunchChannel.getUpdatedBy())) {
            playLaunchChannel.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.createPlayLaunchChannel(tenant.getId(), playName, playLaunchChannel);
    }

    @RequestMapping(value = "/{playName}/channels/{channelId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch channel for a given play")
    public PlayLaunchChannel updatePlayLaunchChannel( //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId, //
            @RequestBody PlayLaunchChannel playLaunchChannel, //
            HttpServletResponse response) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (StringUtils.isEmpty(playLaunchChannel.getCreatedBy())) {
            playLaunchChannel.setCreatedBy(MultiTenantContext.getEmailAddress());
        }
        if (StringUtils.isEmpty(playLaunchChannel.getUpdatedBy())) {
            playLaunchChannel.setUpdatedBy(MultiTenantContext.getEmailAddress());
        }
        return playProxy.updatePlayLaunchChannel(tenant.getId(), playName, channelId, playLaunchChannel);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get play launch for a given play and launch id")
    public PlayLaunch getPlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunch(tenant.getId(), playName, launchId);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}/{action}", //
            method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Update play launch for a given play and launch id with given action")
    public PlayLaunch updatePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @PathVariable("action") LaunchState action) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.updatePlayLaunch(tenant.getId(), playName, launchId, action);
        return playProxy.getPlayLaunch(tenant.getId(), playName, launchId);
    }

    @RequestMapping(value = "/{playName}/launches/{launchId}", method = RequestMethod.DELETE)
    @ResponseBody
    @PreAuthorize("hasRole('Create_PLS_Plays')")
    @ApiOperation(value = "Delete play launch for a given play and launch id")
    public void deletePlayLaunch(@PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam(value = "hardDelete", required = false, defaultValue = "false") Boolean hardDelete) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlayLaunch(tenant.getId(), playName, launchId, hardDelete);
    }

}

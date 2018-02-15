package com.latticeengines.pls.controller;

import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.PlayUtils;
import com.latticeengines.pls.service.RatingEntityPreviewService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

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

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private RatingEntityPreviewService ratingEntityPreviewService;

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
            @RequestParam(value = "endTimestamp", required = false) Long endTimestamp) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboard(tenant.getId(), playName, launchStates, startTimestamp, offset, max,
                sortby, descending, endTimestamp);
    }

    @RequestMapping(value = "/launches/dashboard/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Play entries count for launch dashboard for a tenant")
    public Long getPlayLaunchDashboardEntriesCount(HttpServletRequest request, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays", required = false) //
            @RequestParam(value = "playName", required = false) String playName, //
            @ApiParam(value = "List of launch states to consider", required = false) //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "startTimestamp", required = true) Long startTimestamp, //
            @ApiParam(value = "End date in Unix timestamp", required = false) //
            @RequestParam(value = "endTimestamp", required = false) Long endTimestamp) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunchDashboardEntriesCount(tenant.getId(), playName, launchStates, startTimestamp,
                endTimestamp);
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get full play for a specific tenant based on playName")
    public Play getPlay(@PathVariable String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlay(tenant.getId(), playName);
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
        return playProxy.createOrUpdatePlay(tenant.getId(), play);
    }

    @RequestMapping(value = "/{playName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a play")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public Boolean delete(@PathVariable String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlay(tenant.getId(), playName);
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
        Tenant tenant = MultiTenantContext.getTenant();

        validateNonEmptyTargetsForLaunch(tenant.getId(), playName, playLaunch);

        return playProxy.createPlayLaunch(tenant.getId(), playName, playLaunch);
    }

    @RequestMapping(value = "/{playName}/launches", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get list of launches for a given play")
    public List<PlayLaunch> getPlayLaunches(@PathVariable("playName") String playName, //
            @RequestParam(value = "launchStates", required = false) List<LaunchState> launchStates) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayLaunches(tenant.getId(), playName, launchStates);
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
            @PathVariable("launchId") String launchId) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlayLaunch(tenant.getId(), playName, launchId);
    }

    private void validateNonEmptyTargetsForLaunch(String customerSpace, String playName, PlayLaunch playLaunch) {
        Play play = getPlay(playName);
        PlayUtils.validatePlayBeforeLaunch(play);
        PlayUtils.validatePlayLaunchBeforeLaunch(customerSpace, playLaunch, play);

        RatingEngine ratingEngine = play.getRatingEngine();
        ratingEngine = ratingEngineProxy.getRatingEngine(customerSpace, ratingEngine.getId());
        play.setRatingEngine(ratingEngine);

        DataPage previewDataPage = ratingEntityPreviewService.getEntityPreview( //
                ratingEngine, 0L, 1L, BusinessEntity.Account, //
                playLaunch.getExcludeItemsWithoutSalesforceId(), //
                playLaunch.getBucketsToLaunch().stream() //
                        .map(RatingBucketName::getName) //
                        .collect(Collectors.toList()));

        if (previewDataPage == null || CollectionUtils.isEmpty(previewDataPage.getData())
                || (playLaunch.getTopNCount() != null && playLaunch.getTopNCount() <= 0L)) {
            throw new LedpException(LedpCode.LEDP_18176, new String[] { play.getName() });
        }
    }
}

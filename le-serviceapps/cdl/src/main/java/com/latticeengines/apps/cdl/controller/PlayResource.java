package com.latticeengines.apps.cdl.controller;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingCoverageService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.workflow.CampaignDeltaCalculationWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.CampaignLaunchWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.PlayLaunchWorkflowSubmitter;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.ratings.coverage.RatingBucketCoverage;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.PlayUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "play", description = "REST resource for play")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/plays")
public class PlayResource {

    private static final Logger log = LoggerFactory.getLogger(PlayResource.class);

    @Inject
    private PlayService playService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter;

    @Inject
    private CampaignLaunchWorkflowSubmitter campaignLaunchWorkflowSubmitter;

    @Inject
    private CampaignDeltaCalculationWorkflowSubmitter campaignDeltaCalculationWorkflowSubmitter;

    @Inject
    private RatingCoverageService ratingCoverageService;

    @Inject
    public PlayResource(PlayService playService, PlayLaunchService playLaunchService, MetadataProxy metadataProxy,
            PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter,
            PlayLaunchChannelService playLaunchChannelService) {
        this.playService = playService;
        this.playLaunchService = playLaunchService;
        this.metadataProxy = metadataProxy;
        this.playLaunchWorkflowSubmitter = playLaunchWorkflowSubmitter;
        this.playLaunchChannelService = playLaunchChannelService;
    }

    // -----
    // Plays
    // -----
    @GetMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all full plays for a tenant")
    public List<Play> getPlays( //
            @PathVariable String customerSpace, //
            @RequestParam(value = "should-load-coverage", required = false, defaultValue = "false") Boolean shouldLoadCoverage, //
            @RequestParam(value = "rating-engine-id", required = false) String ratingEngineId) {
        // by default shouldLoadCoverage flag should be false otherwise play
        // listing API takes lot of time to load
        return playService.getAllFullPlays(shouldLoadCoverage, ratingEngineId);
    }

    @GetMapping(value = "/deleted-play-ids", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all deleted play ids for a tenant")
    public List<String> getDeletedPlayIds( //
            @PathVariable String customerSpace, //
            @RequestParam(value = "for-cleanup-only", required = false, defaultValue = "false") Boolean forCleanupOnly) {
        return playService.getAllDeletedPlayIds(forCleanupOnly);
    }

    @GetMapping(value = "/{playName}")
    @ResponseBody
    @ApiOperation(value = "Get full play for a specific tenant based on playName")
    public Play getPlay(//
            @PathVariable String customerSpace, //
            @PathVariable String playName, //
            @RequestParam(value = "should-load-coverage", required = false, defaultValue = "true") Boolean shouldLoadCoverage, //
            @RequestParam(value = "consider-deleted", required = false, defaultValue = "false") Boolean considerDeleted) {
        Play play;
        if (shouldLoadCoverage) {
            play = playService.getFullPlayByName(playName, considerDeleted);
        } else {
            play = playService.getPlayByName(playName, considerDeleted);
        }
        return play;
    }

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Register a play")
    public Play createOrUpdate(//
            @PathVariable String customerSpace, //
            @RequestParam(value = "should-load-coverage", required = false, defaultValue = "true") Boolean shouldLoadCoverage, //
            @RequestBody Play play) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            log.warn("Tenant is null for the request");
            return null;
        }
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Play is null" });
        }

        return playService.createOrUpdate(play, shouldLoadCoverage, tenant.getId());
    }

    @DeleteMapping(value = "/{playName}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a play")
    public Boolean delete( //
            @PathVariable String customerSpace, //
            @PathVariable String playName, //
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        playService.deleteByName(playName, hardDelete == Boolean.TRUE);
        return true;
    }

    // -------------
    // Channels
    // -------------

    @GetMapping(value = "/{playName}/channels")
    @ResponseBody
    @ApiOperation(value = "For the given play, get a list of play launch channels")
    public List<PlayLaunchChannel> getPlayLaunchChannels(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @RequestParam(value = "include-unlaunched-channels", required = false, defaultValue = "false") Boolean includeUnlaunchedChannels) {
        return playLaunchChannelService.getPlayLaunchChannels(playName, includeUnlaunchedChannels);
    }

    @PostMapping(value = "/{playName}/channels", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create play launch channel")
    public PlayLaunchChannel createPlayLaunchChannel( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @RequestBody PlayLaunchChannel playLaunchChannel, //
            @RequestParam(value = "launch-now", required = false, defaultValue = "false") Boolean launchNow) {
        if (playLaunchChannel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Play launch channel object is null" });
        }
        if (playLaunchChannel.getLookupIdMap() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot create a Play launch Channel without a lookupIdMap object" });
        }
        if (playLaunchChannelService.findByPlayNameAndLookupIdMapId(playName,
                playLaunchChannel.getLookupIdMap().getId()) != null) {
            throw new LedpException(LedpCode.LEDP_18220,
                    new String[] { playName, playLaunchChannel.getLookupIdMap().getId() });
        }
        playLaunchChannel = playLaunchChannelService.create(playName, playLaunchChannel);
        if (launchNow) {
            PlayLaunch launch = playLaunchChannelService.queueNewLaunchForChannel(playLaunchChannel.getPlay(),
                    playLaunchChannel);
            log.info(String.format("Queued new launch for play:%s and channel: %s : %s", playName,
                    playLaunchChannel.getId(), launch.getLaunchId()));
        }
        return playLaunchChannel;
    }

    @GetMapping(value = "/{playName}/channels/{channelId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the play launch channel by the given play id and channel id")
    public PlayLaunchChannel getPlayLaunchChannelById(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId) {
        if (StringUtils.isEmpty(channelId)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Empty or blank channel Id" });
        }
        return playLaunchChannelService.findById(channelId);
    }

    @PutMapping(value = "/{playName}/channels/{channelId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a play launch channel for a given play and channel id")
    public PlayLaunchChannel updatePlayLaunchChannel(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId, //
            @RequestParam(value = "launch-now", required = false, defaultValue = "false") Boolean launchNow,
            @RequestBody PlayLaunchChannel playLaunchChannel) {
        if (playLaunchChannel == null) {
            throw new LedpException(LedpCode.LEDP_18219, new String[] { "Invalid play launch channel" });
        }
        if (StringUtils.isEmpty(channelId) || playLaunchChannel.getId() == null) {
            throw new LedpException(LedpCode.LEDP_18219, new String[] { "Empty or blank channel Id" });
        }
        if (!playLaunchChannel.getId().equals(channelId)) {
            throw new LedpException(LedpCode.LEDP_18219,
                    new String[] { "ChannelId is not the same for the Play launch channel being updated" });
        }
        playLaunchChannel = playLaunchChannelService.update(playName, playLaunchChannel);
        if (launchNow) {
            PlayLaunch launch = playLaunchChannelService.queueNewLaunchForChannel(playLaunchChannel.getPlay(),
                    playLaunchChannel);
            log.info(String.format("Queued new launch for play:%s and channel: %s : %s", playName, channelId,
                    launch.getLaunchId()));
        }
        return playLaunchChannel;
    }

    @PatchMapping(value = "/{playName}/channels/{channelId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update next schedule date for play launch channel for a given play and channel id")
    public PlayLaunchChannel updatePlayLaunchChannel(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId) {
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        PlayLaunchChannel channel = playLaunchChannelService.findById(channelId);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Channel found with id: " + channelId });
        }
        channel.setPlay(play);
        Date nextLaunchDate = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        if (nextLaunchDate.after(channel.getExpirationDate())) {
            log.info(String.format(
                    "Channel: %s has expired turning auto launches off (Expiration Date: %s, Next Scheduled Launch Date: %s)",
                    channelId, channel.getExpirationDate().toString(), nextLaunchDate.toString()));
            channel.setIsAlwaysOn(false);
        } else {
            channel.setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(channel));
        }
        channel = playLaunchChannelService.update(play.getName(), channel);
        return channel;
    }

    @PostMapping(value = "/{playName}/channels/{channelId}/launch", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Queue a new Play launch for a given play, channel with delta tables")
    public PlayLaunch queueNewLaunchByPlayAndChannel(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId, //
            @RequestParam(value = PlayUtils.ADDED_ACCOUNTS_DELTA_TABLE, required = false) String addAccountsTable, //
            @RequestParam(value = PlayUtils.REMOVED_ACCOUNTS_DELTA_TABLE, required = false) String removeAccountsTable, //
            @RequestParam(value = PlayUtils.ADDED_CONTACTS_DELTA_TABLE, required = false) String addContactsTable, //
            @RequestParam(value = PlayUtils.REMOVED_CONTACTS_DELTA_TABLE, required = false) String removeContactsTable, //
            @RequestParam(value = "is-auto-launch", required = false) boolean isAutoLaunch) {
        if (StringUtils.isEmpty(playName)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Empty or blank Play Id" });
        }
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18151, new String[] { playName });
        }

        PlayLaunchChannel channel = playLaunchChannelService.findById(channelId);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No channel found by channel id: " + channelId });
        }

        channel.setPlay(play);
        channel.setTenant(MultiTenantContext.getTenant());
        channel.setTenantId(MultiTenantContext.getTenant().getPid());
        return playLaunchChannelService.queueNewLaunchForChannel(play, channel, addAccountsTable, removeAccountsTable,
                addContactsTable, removeContactsTable, isAutoLaunch);
    }

    @PostMapping(value = "/{playName}/channels/{channelId}/kickoff-workflow", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Internal only use API to kick off campaignLaunch workflow immediately for a channel")
    @Deprecated
    public String kickOffWorkflowFromChannel(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId) {
        PlayLaunch playLaunch = queueNewLaunchByPlayAndChannel(customerSpace, playName, channelId, null, null, null,
                null, false);
        return kickOffLaunch(customerSpace, playName, playLaunch.getId());
    }

    @PostMapping(value = "/{playName}/channels/{channelId}/kickoff-delta-calculation", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a play launch channel for a given play and channel id")
    public String schedule(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("channelId") String channelId) {
        return campaignDeltaCalculationWorkflowSubmitter.submit(customerSpace, playName, channelId).toString();
    }

    // -------------
    // Play Launches
    // -------------

    @GetMapping(value = "/{playName}/launches")
    @ResponseBody
    @ApiOperation(value = "Get list of launches for a given play")
    public List<PlayLaunch> getPlayLaunches(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @RequestParam(value = "launch-state", required = false) List<LaunchState> launchStates) {
        return playLaunchService.findByPlayId(getPlayId(playName), launchStates);
    }

    @GetMapping(value = "/{playName}/launches/{launchId}/channel")
    @ResponseBody
    @ApiOperation(value = "Get play launch channel from play launch")
    public PlayLaunchChannel getPlayLaunchChannelFromPlayLaunch(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        return playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
    }

    @PostMapping(value = "/{playName}/launches", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create play launch for a given play")
    public PlayLaunch createPlayLaunch( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, @RequestBody PlayLaunch playLaunch) {
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Play launch object is null" });
        }

        if (playLaunch.getDestinationOrgId() == null || playLaunch.getDestinationSysType() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot create a Play launch without Destination Org  or Destination System" });
        }

        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
        playLaunch.setLaunchState(LaunchState.UnLaunched);
        playLaunch.setPlay(play);
        playLaunchService.create(playLaunch);
        return playLaunch;
    }

    @GetMapping(value = "/{playName}/launches/{launchId}")
    @ResponseBody
    @ApiOperation(value = "Get play launch for a given play and launch id")
    public PlayLaunch getPlayLaunch(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        getPlayId(playName);
        return playLaunchService.findByLaunchId(launchId);
    }

    @PostMapping(value = "/{playName}/launches/{launchId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a play launch for a given play")
    public PlayLaunch updatePlayLaunch(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestBody PlayLaunch playLaunch, //
            HttpServletResponse response) {
        if (StringUtils.isEmpty(launchId)) {
            throw new LedpException(LedpCode.LEDP_18205, new String[] { "empty or blank launch Id" });
        }
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_18205, new String[] { "Invalid play launch" });
        }
        if (!playLaunch.getId().equals(launchId)) {
            throw new LedpException(LedpCode.LEDP_18205,
                    new String[] { "LaunchId is not the same for the Play launch being updated" });
        }
        if (playLaunch.getDestinationOrgId() == null || playLaunch.getDestinationSysType() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot update a Play launch without Destination Org  or Destination System" });
        }

        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            log.error("Invalid playName: " + playName);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        playLaunch.setPlay(play);
        playLaunch.setTenant(MultiTenantContext.getTenant());
        playLaunch.setTenantId(MultiTenantContext.getTenant().getPid());
        return playLaunchService.update(playLaunch);
    }

    @PostMapping(value = "/{playName}/launches/{launchId}/kickoff-launch", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Launch a play launch for a given play")
    public String kickOffLaunch(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId) {
        if (StringUtils.isEmpty(playName)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Empty or blank play Id" });
        }
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18151, new String[] { playName });
        }
        PlayUtils.validatePlay(play);

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No launch found by launchId: " + launchId });
        }
        playLaunch.setPlay(play);
        PlayUtils.validatePlayLaunchBeforeLaunch(playLaunch, play);
        if (play.getRatingEngine() != null) {
            validateNonEmptyTargetsForLaunch(customerSpace, play, playLaunch);
        }
        if (playLaunch.getLaunchState() != LaunchState.Queued) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { String
                    .format("Launch %s is not in Queued state and hence launch cannot be kicked off", launchId) });
        }
        return campaignLaunchWorkflowSubmitter.submit(playLaunch).toString();

    }

    @PostMapping(value = "/{playName}/launches/{launchId}/launch", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Launch a play launch for a given play")
    public PlayLaunch launchPlay(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam(value = "dry-run", required = false, defaultValue = "false") //
            boolean isDryRunMode, //
            @RequestParam(value = "use-spark", required = false, defaultValue = "false") //
            boolean useSpark) {
        if (StringUtils.isEmpty(playName)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Empty or blank play Id" });
        }
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_18151, new String[] { playName });
        }
        PlayUtils.validatePlay(play);

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No launch found by launchId: " + launchId });
        }
        playLaunch.setPlay(play);
        PlayUtils.validatePlayLaunchBeforeLaunch(playLaunch, play);
        if (play.getRatingEngine() != null) {
            validateNonEmptyTargetsForLaunch(customerSpace, play, playLaunch);
        }
        // this dry run flag is useful in writing robust testcases
        if (!isDryRunMode) {
            String appId;
            if (useSpark) {
                appId = campaignLaunchWorkflowSubmitter.submit(playLaunch).toString();
            } else {
                appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
            }
            playLaunch.setApplicationId(appId);
        }

        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
        playLaunch.setTableName(createTable(playLaunch));

        Long totalAvailableRatedAccounts = play.getTargetSegment().getAccounts();
        Long totalAvailableContacts = play.getTargetSegment().getContacts();

        playLaunch.setAccountsSelected(totalAvailableRatedAccounts != null ? totalAvailableRatedAccounts : 0L);
        playLaunch.setAccountsSuppressed(0L);
        playLaunch.setAccountsErrored(0L);
        playLaunch.setAccountsLaunched(0L);
        playLaunch.setContactsSelected(totalAvailableContacts != null ? totalAvailableContacts : 0L);
        playLaunch.setContactsLaunched(0L);
        playLaunch.setContactsSuppressed(0L);
        playLaunch.setContactsErrored(0L);
        return playLaunchService.update(playLaunch);
    }

    @GetMapping(value = "/launches/dashboard", headers = "Accept=application/json")
    @ResponseBody
    public PlayLaunchDashboard getPlayLaunchDashboard( //
            @PathVariable String customerSpace, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays") //
            @RequestParam(value = "play-name", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type") //
            @RequestParam(value = "org-id", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type") //
            @RequestParam(value = "external-sys-type", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider") //
            @RequestParam(value = "launch-state", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "start-timestamp") Long startTimestamp, //
            @ApiParam(value = "Play launch offset from start time", required = true) //
            @RequestParam(value = "offset") Long offset, //
            @ApiParam(value = "Maximum number of play launches to consider", required = true) //
            @RequestParam(value = "max") Long max, //
            @ApiParam(value = "Sort by") //
            @RequestParam(value = "sortby", required = false) String sortby, //
            @ApiParam(value = "Sort in descending order", defaultValue = "true") //
            @RequestParam(value = "descending", required = false, defaultValue = "true") boolean descending, //
            @ApiParam(value = "End date in Unix timestamp") //
            @RequestParam(value = "end-timestamp", required = false) Long endTimestamp) {
        return playLaunchService.getDashboard(getPlayId(playName), launchStates, startTimestamp, offset, max, sortby,
                descending, endTimestamp, orgId, externalSysType, false);
    }

    @GetMapping(value = "/launches/dashboard/count", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Play entries count for launch dashboard for a tenant")
    public Long getPlayLaunchDashboardEntriesCount( //
            @PathVariable String customerSpace, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays") //
            @RequestParam(value = "play-name", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type") //
            @RequestParam(value = "org-id", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type") //
            @RequestParam(value = "external-sys-type", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider") //
            @RequestParam(value = "launch-state", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "start-timestamp") Long startTimestamp, //
            @ApiParam(value = "End date in Unix timestamp") //
            @RequestParam(value = "end-timestamp", required = false) Long endTimestamp) {
        return playLaunchService.getDashboardEntriesCount(getPlayId(playName), launchStates, startTimestamp,
                endTimestamp, orgId, externalSysType);
    }

    @PatchMapping(value = "/{playName}/launches/{launchId}")
    @ResponseBody
    @ApiOperation(value = "Update play launch progress.")
    public PlayLaunch updatePlayLaunchProgress(//
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam("launchCompletionPercent") Double launchCompletionPercent, //
            @RequestParam("accountsLaunched") Long accountsLaunched, //
            @RequestParam("contactsLaunched") Long contactsLaunched, //
            @RequestParam("accountsErrored") Long accountsErrored, //
            @RequestParam("accountsSuppressed") Long accountsSuppressed) {
        log.debug(String.format("Record play launch progress for %s launchId", launchId));

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        playLaunch.setAccountsLaunched(accountsLaunched);
        playLaunch.setAccountsErrored(accountsErrored);
        playLaunch.setContactsLaunched(contactsLaunched);
        playLaunch.setLaunchCompletionPercent(launchCompletionPercent);
        playLaunch.setAccountsSuppressed(accountsSuppressed);
        return playLaunchService.update(playLaunch);
    }

    @PutMapping(value = "/{playName}/launches/{launchId}/{action}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update play launch for a given play and launch id with given action")
    public PlayLaunch updatePlayLaunch( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @PathVariable("action") LaunchState action) {
        getPlayId(playName);
        PlayLaunch existingPlayLaunch = playLaunchService.findByLaunchId(launchId);
        if (existingPlayLaunch != null) {
            if (LaunchState.canTransit(existingPlayLaunch.getLaunchState(), action)) {
                existingPlayLaunch.setLaunchState(action);
                return playLaunchService.update(existingPlayLaunch);
            }
        }
        return existingPlayLaunch;
    }

    @DeleteMapping(value = "/{playName}/launches/{launchId}")
    @ResponseBody
    @ApiOperation(value = "Delete play launch for a given play and launch id")
    public void deletePlayLaunch( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam(value = "hard-delete", required = false, defaultValue = "false") Boolean hardDelete) {
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        if (playLaunch != null) {
            playLaunchService.deleteByLaunchId(launchId, hardDelete == Boolean.TRUE);
        }
    }

    // --------------
    // Talking Points
    // --------------
    @PostMapping(value = "/{playName}/talkingpoints/publish", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Publish Talking Points for a given play")
    public void publishTalkingPoints( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName) {
        playService.publishTalkingPoints(playName, customerSpace);
    }

    private String createTable(PlayLaunch playLaunch) {
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());

        Table generatedRecommendationTable = new Table();
        generatedRecommendationTable.addAttributes(Recommendation.getSchemaAttributes());

        String tableName = "play_launch_" + UUID.randomUUID().toString().replaceAll("-", "_");
        generatedRecommendationTable.setName(tableName);
        generatedRecommendationTable.setTableType(TableType.DATATABLE);

        generatedRecommendationTable.setDisplayName("Play Launch recommendation");
        generatedRecommendationTable.setTenant(MultiTenantContext.getTenant());
        generatedRecommendationTable.setTenantId(MultiTenantContext.getTenant().getPid());
        generatedRecommendationTable.setMarkedForPurge(false);
        metadataProxy.createTable(customerSpace.toString(), tableName, generatedRecommendationTable);

        generatedRecommendationTable = metadataProxy.getTable(customerSpace.toString(), tableName);

        return generatedRecommendationTable.getName();
    }

    private Long getPlayId(String playName) {
        Long playId = null;
        if (StringUtils.isNotBlank(playName)) {
            Play play = playService.getPlayByName(playName, false);
            if (play == null) {
                throw new LedpException(LedpCode.LEDP_18151, new String[] { playName });
            }
            playId = play.getPid();
        }
        return playId;
    }

    private void validateNonEmptyTargetsForLaunch(String customerSpace, Play play, PlayLaunch playLaunch) {
        RatingEngine ratingEngine = play.getRatingEngine();
        ratingEngine = ratingEngineService.getRatingEngineById(ratingEngine.getId(), false);
        play.setRatingEngine(ratingEngine);

        RatingEnginesCoverageRequest coverageRequest = new RatingEnginesCoverageRequest();
        coverageRequest.setRatingEngineIds(Collections.singletonList(play.getRatingEngine().getId()));
        coverageRequest.setRestrictNullLookupId(playLaunch.getExcludeItemsWithoutSalesforceId());
        coverageRequest.setLookupId(playLaunch.getDestinationAccountId());
        RatingEnginesCoverageResponse coverageResponse = ratingCoverageService
                .getRatingCoveragesForSegment(customerSpace, play.getTargetSegment().getName(), coverageRequest);

        if (coverageResponse == null || MapUtils.isNotEmpty(coverageResponse.getErrorMap())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Unable to validate validity of launch targets due to internal Error, please retry later" });
        }

        Long accountsToLaunch = coverageResponse.getRatingModelsCoverageMap().get(play.getRatingEngine().getId())
                .getBucketCoverageCounts().stream()
                .filter(ratingBucket -> playLaunch.getBucketsToLaunch()
                        .contains(RatingBucketName.valueOf(ratingBucket.getBucket())))
                .map(RatingBucketCoverage::getCount).reduce(0L, (a, b) -> a + b);

        accountsToLaunch = accountsToLaunch
                + (playLaunch.isLaunchUnscored()
                        ? coverageResponse.getRatingModelsCoverageMap().get(play.getRatingEngine().getId())
                                .getUnscoredAccountCount()
                        : 0L);

        if (accountsToLaunch <= 0L) {
            throw new LedpException(LedpCode.LEDP_18176, new String[] { play.getName() });
        }
    }
}

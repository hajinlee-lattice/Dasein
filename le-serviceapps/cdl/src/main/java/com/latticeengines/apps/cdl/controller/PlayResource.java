package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

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

import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.service.RatingEntityPreviewService;
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
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
    private RatingEngineService ratingEngineService;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter;

    @Inject
    private RatingEntityPreviewService ratingEntityPreviewService;

    @Inject
    public PlayResource(PlayService playService, PlayLaunchService playLaunchService, MetadataProxy metadataProxy,
            PlayLaunchWorkflowSubmitter playLaunchWorkflowSubmitter) {
        this.playService = playService;
        this.playLaunchService = playLaunchService;
        this.metadataProxy = metadataProxy;
        this.playLaunchWorkflowSubmitter = playLaunchWorkflowSubmitter;
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
            throw new NullPointerException("Play is null");
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
    // -----
    // Plays
    // -----

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

    @PostMapping(value = "/{playName}/launches", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create play launch for a given play")
    public PlayLaunch createPlayLaunch( //
            @PathVariable String customerSpace, //
            @PathVariable("playName") String playName, @RequestBody PlayLaunch playLaunch) {
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Play launch object is null" });
        }

        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found with id: " + playName });
        }

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

        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            log.error("Invalid playName: " + playName);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        return playLaunchService.update(playLaunch);
    }

    @PostMapping(value = "/{playName}/launches/{launchId}/launch", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a play launch for a given play")
    public PlayLaunch launchPlay(@PathVariable String customerSpace, //
            @PathVariable("playName") String playName, //
            @PathVariable("launchId") String launchId, //
            @RequestParam(value = "dry-run", required = false, defaultValue = "false") //
            boolean isDryRunMode) {
        if (StringUtils.isEmpty(launchId)) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "Empty or blank launch Id" });
        }
        Play play = playService.getPlayByName(playName, false);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No Play found by play Id: " + playName });
        }
        PlayUtils.validatePlay(playName, play);

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId);
        if (playLaunch == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No launch found by launchId: " + launchId });
        }
        PlayUtils.validatePlayLaunchBeforeLaunch(customerSpace, playLaunch, play);
        validateNonEmptyTargetsForLaunch(play, playName, playLaunch, //
                playLaunch.getDestinationAccountId());

        // this dry run flag is useful in writing robust testcases
        if (!isDryRunMode) {
            String appId = playLaunchWorkflowSubmitter.submit(playLaunch).toString();
            playLaunch.setApplicationId(appId);
        }

        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
        playLaunch.setTableName(createTable(playLaunch));
        List<String> allAvailableBuckets = Arrays.asList(RatingBucketName.values()).stream().map(RatingBucketName::name)
                .collect(Collectors.toList());
        Long totalAvailableRatedAccounts = ratingEntityPreviewService.getEntityPreviewCount(play.getRatingEngine(),
                BusinessEntity.Account, false, null, allAvailableBuckets, null);
        playLaunch.setAccountsSelected(totalAvailableRatedAccounts);
        playLaunch.setAccountsSuppressed(0L);
        playLaunch.setAccountsErrored(0L);
        playLaunch.setAccountsLaunched(0L);
        playLaunch.setContactsLaunched(0L);
        return playLaunchService.update(playLaunch);
    }

    @GetMapping(value = "/launches/dashboard", headers = "Accept=application/json")
    @ResponseBody
    public PlayLaunchDashboard getPlayLaunchDashboard( //
            @PathVariable String customerSpace, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays", required = false) //
            @RequestParam(value = "play-name", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "org-id", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "external-sys-type", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider", required = false) //
            @RequestParam(value = "launch-state", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "start-timestamp", required = true) Long startTimestamp, //
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
        return playLaunchService.getDashboard(getPlayId(playName), launchStates, startTimestamp, offset, max, sortby,
                descending, endTimestamp, orgId, externalSysType, false);
    }

    @GetMapping(value = "/launches/dashboard/count", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Play entries count for launch dashboard for a tenant")
    public Long getPlayLaunchDashboardEntriesCount( //
            @PathVariable String customerSpace, //
            @ApiParam(value = "Play name for which to load dashboard info. Empty play name means dashboard " //
                    + "should consider play launches across all plays", required = false) //
            @RequestParam(value = "play-name", required = false) String playName, //
            @ApiParam(value = "Org id for which to load dashboard info. Empty org id means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "org-id", required = false) String orgId, //
            @ApiParam(value = "External system type for which to load dashboard info. Empty external system type means dashboard " //
                    + "should consider play launches across all org ids and external system type", required = false) //
            @RequestParam(value = "external-sys-type", required = false) String externalSysType, //
            @ApiParam(value = "List of launch states to consider", required = false) //
            @RequestParam(value = "launch-state", required = false) List<LaunchState> launchStates, //
            @ApiParam(value = "Start date in Unix timestamp", required = true) //
            @RequestParam(value = "start-timestamp", required = true) Long startTimestamp, //
            @ApiParam(value = "End date in Unix timestamp", required = false) //
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
    // -------------
    // Play Launches
    // -------------

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
    // --------------
    // Talking Points
    // --------------

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

    private void validateNonEmptyTargetsForLaunch(Play play, String playName, //
            PlayLaunch playLaunch, String lookupIdColumn) {
        RatingEngine ratingEngine = play.getRatingEngine();
        ratingEngine = ratingEngineService.getRatingEngineById(ratingEngine.getId(), false);
        play.setRatingEngine(ratingEngine);

        Long previewDataCount = ratingEntityPreviewService.getEntityPreviewCount( //
                ratingEngine, BusinessEntity.Account, //
                playLaunch.getExcludeItemsWithoutSalesforceId(), //
                null,
                playLaunch.getBucketsToLaunch().stream() //
                        .map(RatingBucketName::getName) //
                        .collect(Collectors.toList()),
                lookupIdColumn);

        if (previewDataCount == null || previewDataCount <= 0L) {
            throw new LedpException(LedpCode.LEDP_18176, new String[] { play.getName() });
        }
    }
}

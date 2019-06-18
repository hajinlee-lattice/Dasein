package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.annotation.PodContextAware;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.exposed.service.MatchValidationService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.datacloud.match.BulkMatchWorkflowConfiguration;
import com.latticeengines.matchapi.service.BulkMatchService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "match", description = "REST resource for propdata matches")
@RestController
@RequestMapping("/matches")
public class MatchResource {
    private static final Logger log = LoggerFactory.getLogger(MatchResource.class);

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private List<BulkMatchService> bulkMatchServiceList;

    @Resource(name = "bulkMatchServiceWithAccountMaster")
    private BulkMatchService defaultBulkMatchService;

    @Inject
    private MatchValidationService matchValidationService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    @Inject
    private EntityMatchInternalService entityInternalMatchService;

    @Inject
    private DataCloudVersionService datacloudVersionService;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @PostMapping(value = "/realtime")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. "
    )
    public MatchOutput matchRealTime(@RequestBody MatchInput input) {
        try {
            setDataCloudVersion(input, null);
            matchValidationService.validateDataCloudVersion(input.getDataCloudVersion(), input.getTenant());
            clearAllocateModeFlag(input);

            // Skip logic for setting up mock for CDL lookup if MatchInput has
            // Operational Mode set to LDC Match or
            // Entity Match.
            if (input.getOperationalMode() == null || OperationalMode.CDL_LOOKUP.equals(input.getOperationalMode())) {
                if (MapUtils.isNotEmpty(input.getKeyMap()) && input.getKeyMap().containsKey(MatchKey.LookupId) //
                        && !"AccountId".equals(input.getKeyMap().get(MatchKey.LookupId).get(0))) {
                    input = mockForCDLLookup(input);
                }
            }
            return realTimeMatchService.match(input);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData matchRealTime failed.", e);
        }
    }

    @PostMapping(value = "/bulkrealtime")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Specify input fields and MatchKey -> Field mapping. "
            + "Available match keys are Domain, Name, City, State, Country, DUNS, LatticeAccountID. "
            + "Domain can be anything that can be parsed to a domain, such as website, email, etc. "
            + "When domain is not provided, Name, State, Country must be provided. Country is default to USA. ")
    public BulkMatchOutput bulkMatchRealTime(@RequestBody BulkMatchInput input) {
        long time = System.currentTimeMillis();
        try {
            if (CollectionUtils.isNotEmpty(input.getInputList())) {
                String datacloudVersion = datacloudVersionService.currentApprovedVersion().getVersion();
                for (MatchInput matchInput : input.getInputList()) {
                    clearAllocateModeFlag(matchInput);
                    setDataCloudVersion(matchInput, datacloudVersion);
                    matchValidationService.validateDataCloudVersion(matchInput.getDataCloudVersion(),
                            matchInput.getTenant());
                }
            }
            return realTimeMatchService.matchBulk(input);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData matchBulk failed.", e);
        } finally {
            log.info((System.currentTimeMillis() - time) + " milli for matching " + input.getInputList().size()
                    + " match inputs");
        }
    }

    @PodContextAware
    @PostMapping(value = "/bulk", produces = "application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Same input as realtime match, "
            + "except using InputBuffer instead of embedding Data in json body directly. "
            + "The request parameter podid is used to change the hdfs pod id. "
            + "This parameter is mainly for testing purpose. "
            + "Leave it empty will result in using the pod id defined in camille environment.")
    public MatchCommand matchBulk(@RequestBody MatchInput input,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            setDataCloudVersion(input, null);
            String datacloudVersion = input.getDataCloudVersion();
            matchValidationService.validateDataCloudVersion(datacloudVersion, input.getTenant());
            matchValidationService.validateDecisionGraph(input.getDecisionGraph());
            if (input.bumpupEntitySeedVersion()) {
                entityMatchVersionService.bumpVersion(EntityMatchEnvironment.STAGING, input.getTenant());
            }
            BulkMatchService bulkMatchService = getBulkMatchService(datacloudVersion);
            return bulkMatchService.match(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData matchBulk failed: " + e.getMessage(), e);
        }
    }

    /**
     * @param input
     * @param hdfsPod
     * @return
     */
    @PodContextAware
    @PostMapping(value = "/bulkconf", produces = "application/json")
    @ResponseBody
    @ApiOperation(value = "Match to derived column selection. Same input as realtime match, "
            + "except using InputBuffer instead of embedding Data in json body directly. "
            + "The request parameter podid is used to change the hdfs pod id. "
            + "This parameter is mainly for testing purpose. "
            + "Leave it empty will result in using the pod id defined in camille environment.")
    public BulkMatchWorkflowConfiguration getBulkMatchConfig(@RequestBody MatchInput input,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            setDataCloudVersion(input, null);
            String datacloudVersion = input.getDataCloudVersion();
            matchValidationService.validateDataCloudVersion(datacloudVersion, input.getTenant());
            BulkMatchService bulkMatchService = getBulkMatchService(datacloudVersion);
            return bulkMatchService.getWorkflowConf(input, hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25007, "PropData getBulkMatchConfig failed: " + e.getMessage(), e);
        }
    }

    @GetMapping(value = "/bulk/{rootuid}", produces = "application/json")
    @ResponseBody
    @ApiOperation(value = "Get match status using rootuid (RootOperationUid).")
    public MatchCommand bulkMatchStatus(@PathVariable String rootuid) {
        try {
            return defaultBulkMatchService.status(rootuid.toUpperCase());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25008, e, new String[] { rootuid });
        }
    }

    @PostMapping(value = "/entity/publish")
    @ResponseBody
    @ApiOperation(value = "Publish entity seed/lookup entries "
            + "from source tenant (staging env) to dest tenant (staging/serving env). "
            + "Only support small-scale publish (approx. <= 10K seeds).")
    public EntityPublishStatistics publishEntity(@RequestBody EntityPublishRequest request) {
        try {
            validateEntityPublishRequest(request);
            if (request.isBumpupVersion()) {
                entityMatchVersionService.bumpVersion(request.getDestEnv(), request.getDestTenant());
            }
            EntityPublishStatistics statistics = entityInternalMatchService.publishEntity(request.getEntity(),
                    request.getSrcTenant(), request.getDestTenant(), request.getDestEnv(), request.getDestTTLEnabled());
            statistics.setRequest(request);
            return statistics;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25042, e);
        }
    }

    @PostMapping(value = "/entity/publish/list")
    @ResponseBody
    @ApiOperation(value = "Serve multiple requests to publish entity seed/lookup entries "
            + "from source tenant (staging env) to dest tenant (staging/serving env). "
            + "Only support small-scale publish (approx. <= 10K seeds). ")
    public List<EntityPublishStatistics> publishEntity(@RequestBody List<EntityPublishRequest> requests) {
        try {
            validateEntityPublishRequests(requests);
            List<EntityPublishStatistics> stats = new ArrayList<>();
            // Don't introduce parallelism here. Order is enforced. Succeeding
            // publish request might have dependency on preceding requests as
            // data published first will be overwritten by data published later
            requests.forEach(request -> {
                stats.add(publishEntity(request));
            });
            return stats;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25042, e);
        }
    }

    @PostMapping(value = "/entity/versions")
    @ResponseBody
    @ApiOperation(value = "Bump up entity match version of a target tenant in a list of specified environments")
    public BumpVersionResponse bumpVersion(@RequestBody BumpVersionRequest request) {
        validateBumpVersionRequest(request);
        Tenant tenant = request.getTenant();
        // env => entity match version after bump up operation
        Map<EntityMatchEnvironment, Integer> versionMap = request.getEnvironments().stream().distinct().map(env -> {
            int version = entityMatchVersionService.bumpVersion(env, tenant);
            return Pair.of(env, version);
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v1));

        // generate response
        BumpVersionResponse response = new BumpVersionResponse();
        response.setTenant(request.getTenant());
        response.setVersions(versionMap);
        return response;
    }

    private void validateEntityPublishRequest(EntityPublishRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("Please provide non-empty entity publish request");
        }
        if (StringUtils.isBlank(request.getEntity())) {
            throw new IllegalArgumentException("Please provide entity");
        }
        if (request.getSrcTenant() == null || StringUtils.isBlank(request.getSrcTenant().getId())) {
            throw new IllegalArgumentException("Please provide source tenant with valid tenant id");
        }
        if (request.getDestTenant() == null || StringUtils.isBlank(request.getDestTenant().getId())) {
            throw new IllegalArgumentException("Please provide dest tenant with valid tenant id");
        }
        if (request.getDestEnv() == null) {
            throw new IllegalArgumentException("Please provide valid dest environment");
        }
        if (request.getSrcTenant().getId().equals(request.getDestTenant().getId())
                && EntityMatchEnvironment.STAGING == request.getDestEnv()) {
            throw new IllegalArgumentException("Publish within staging env for same tenant is not allowed");
        }
    }

    private void validateEntityPublishRequests(List<EntityPublishRequest> requests) {
        if (CollectionUtils.isEmpty(requests) || requests.contains(null)) {
            throw new IllegalArgumentException("EntityPublishRequest cannot be empty");
        }
        // Validate all requests first instead of finishing some publish
        // requests and failing validation in the middle.
        requests.forEach(request -> {
            validateEntityPublishRequest(request);
        });
    }

    private void validateBumpVersionRequest(BumpVersionRequest request) {
        Preconditions.checkNotNull(request, "BumpVersionRequest should not be null");
        Preconditions.checkArgument(request.getTenant() != null && request.getTenant().getId() != null,
                "Missing tenant with valid tenant id in BumpVersionRequest");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(request.getEnvironments()),
                "Missing list of entity match environments in BumpVersionRequest");
        request.getEnvironments().forEach(
                env -> Preconditions.checkNotNull(env, "Should not have null environment in BumpVersionRequest"));
    }

    private BulkMatchService getBulkMatchService(String matchVersion) {
        for (BulkMatchService handler : bulkMatchServiceList) {
            if (handler.accept(matchVersion)) {
                return handler;
            }
        }
        throw new LedpException(LedpCode.LEDP_25021, new String[] { matchVersion });
    }

    /*
     * set allocateId field to false (lookup mode) for real time match
     */
    private void clearAllocateModeFlag(MatchInput input) {
        if (input == null) {
            return;
        }

        if (input.isAllocateId()) {
            // log warning if we get allocateId = true in realtime match
            log.warn("Cannot be in allocate mode for realtime match, set to lookup mode. MatchInput={}", input);
        }
        // set to non-allocate (lookup) mode
        input.setAllocateId(false);
    }

    private MatchInput mockForCDLLookup(MatchInput input) {
        input.setCustomSelection(null);
        input.setUnionSelection(null);
        input.setPredefinedSelection(ColumnSelection.Predefined.RTS);
        input.setFetchOnly(true);
        List<String> idFields = new ArrayList<>(input.getKeyMap().get(MatchKey.LookupId));
        input.setKeyMap(ImmutableMap.of(MatchKey.LatticeAccountID, idFields));
        input.setSkipKeyResolution(true);
        return input;
    }

    /**
     * Set default DataCloud version if it's not provided in MatchInput.
     *
     * Current default DataCloud version is latest approved version with major
     * version as 2.0
     *
     * @param input
     */
    private void setDataCloudVersion(MatchInput input, String datacloudVersion) {
        if (StringUtils.isBlank(input.getDataCloudVersion())) {
            if (StringUtils.isBlank(datacloudVersion)) {
                datacloudVersion = datacloudVersionService.currentApprovedVersion().getVersion();
            }
            log.warn("Found a match request without DataCloud version, force to use {}. MatchInput={}",
                    datacloudVersion,
                    JsonUtils.serialize(input));
            input.setDataCloudVersion(datacloudVersion);
        }
    }
}

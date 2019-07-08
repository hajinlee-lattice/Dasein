package com.latticeengines.matchapi.controller;


import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.PatchBookEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.PatchBookValidator;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;
import com.latticeengines.domain.exposed.datacloud.match.patch.LookupPatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.patch.LookupPatchResponse;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchBookValidationError;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchLog;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchMode;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchRequest;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchResponse;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchStatus;
import com.latticeengines.domain.exposed.datacloud.match.patch.PatchValidationResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "patch", description = "REST resource for account master lookup patch")
@RestController
@RequestMapping("/patches")
public class PatchResource {

    private static final Set<PatchStatus> STATUSES_ALLOWED_IN_RESPONSE = Sets.newHashSet(
            PatchStatus.Failed, PatchStatus.Noop, PatchStatus.NewInactive);

    @Inject
    private DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Inject
    private PatchBookEntityMgr patchBookEntityMgr;

    @Inject
    private PatchBookValidator patchBookValidator;

    @Inject
    private PatchService patchService;

    private static final String MIN_PID = "MIN";
    private static final String MAX_PID = "MAX";

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Batch update a list of lookup entries", response = LookupUpdateResponse.class)
    private LookupUpdateResponse patch(@RequestBody List<LookupUpdateRequest> requests) {
        return patchService.patch(requests);
    }

    @RequestMapping(
            value = "/validate/{patchBookType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate patch book entries with the given type", response = PatchValidationResponse.class)
    private PatchValidationResponse validatePatchBook(@PathVariable String patchBookType, @RequestBody PatchRequest request) {
        checkRequired(request);
        checkAndSetDataCloudVersion(request);

        PatchBook.Type type = getPatchBookType(patchBookType);
        List<PatchBook> books = load(request.getMode(), type, request.getOffset(),
                request.getLimit(), request.getSortByField());
        return validate(books, type, request);
    }
    
    @RequestMapping(
            value = "/validatePagination/{patchBookType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate patch book entries with the given type", response = PatchValidationResponse.class)
    private PatchValidationResponse validatePatchBookWithPagin(@PathVariable String patchBookType, @RequestBody PatchRequest request) {
        checkRequired(request);
        checkAndSetDataCloudVersion(request);

        PatchBook.Type type = getPatchBookType(patchBookType);
        List<PatchBook> books = loadWithPagin(request.getMode(), type, request.getOffset(),
                request.getLimit(), request.getPid());
        return validate(books, type, request);
    }

    @RequestMapping(value = "/findMinMaxPid/{patchBookType}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Validate patch book entries with the given type", response = PatchValidationResponse.class)
    private PatchValidationResponse findMinMaxPid(@PathVariable String patchBookType,
            @RequestBody PatchRequest request) {
        checkRequired(request);
        checkAndSetDataCloudVersion(request);

        PatchBook.Type type = getPatchBookType(patchBookType);
        Map<String, Long> pids = loadMinMaxPid(type, PatchBook.COLUMN_PID);
        return configMinMaxPidResponse(pids, type, request);
    }

    @RequestMapping(value = "/lookup", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Patch all patch book entries with lookup type", response = LookupPatchResponse.class)
    private LookupPatchResponse lookupPatch(@RequestBody LookupPatchRequest request) {
        checkRequired(request);
        checkAndSetDataCloudVersion(request);

        LookupPatchResponse response = prepareResponse(request);

        PatchBook.Type type = PatchBook.Type.Lookup;
        List<PatchBook> books = load(request.getMode(), type, request.getOffset(),
                request.getLimit(), request.getSortByField());

        // validate
        PatchValidationResponse validationResponse = validate(books, type, request);
        response.setValidationResponse(validationResponse);
        if (!validationResponse.isSuccess()) {
            // failed validation, do nothing
            return endPatchResponse(response);
        }

        // patch entries
        List<PatchLog> logs = patchService.lookupPatch(
                books, request.getDataCloudVersion(), request.getMode(), request.isDryRun());
        // only return important logs as returning all of them will be too much
        // full logs will be available in log file
        response.setNotPatchedLogs(logs
                .stream()
                .filter(log -> STATUSES_ALLOWED_IN_RESPONSE.contains(log.getStatus()))
                .collect(Collectors.toList()));

        // upload logs
        response.setLogFile(
                patchService.uploadPatchLog(
                        response.getMode(), response.getPatchBookType(), response.isDryRun(),
                        response.getDataCloudVersion(), response.getStartAt(), logs));

        // calculate stats
        response.setStats(calculateStats(logs));
        // cleanup fields
        response.getNotPatchedLogs().forEach(this::cleanupPatchLog);

        return endPatchResponse(response);
    }

    /*
     * Generate patch stats (total # of items, # of items for each PatchStatus)
     */
    private PatchResponse.PatchStats calculateStats(@NotNull List<PatchLog> patchLogs) {
        PatchResponse.PatchStats stats = new PatchResponse.PatchStats();
        stats.setTotal(patchLogs.size());
        stats.setStatusMap(patchLogs
                .stream()
                // count the frequency
                .collect(Collectors.toMap(PatchLog::getStatus, l -> 1, Integer::sum)));
        return stats;
    }

    /*
     * trim debug fields (not returning to prevent API response from being too large)
     */
    private void cleanupPatchLog(@NotNull PatchLog patchLog) {
        patchLog.setInputMatchKey(null);
        patchLog.setOriginalValue(null);
        patchLog.setPatchedValue(null);
    }

    /*
     * set endAt and calculate duration
     */
    private LookupPatchResponse endPatchResponse(@NotNull LookupPatchResponse response) {
        response.setEndAt(new Date());
        Date end = response.getEndAt();
        Date start = response.getStartAt();
        // duration.toString() => ISO-8601 e.g., PT8H6M12.345S
        Duration duration = Duration.between(start.toInstant(), end.toInstant());
        // remove PT & change unit to lowercase => 8h6m12.345s
        String durationStr = duration.toString().substring(2).toLowerCase();
        // TODO maybe remove sub-second part
        response.setDuration(durationStr);
        return response;
    }

    /*
     * configure response with input request
     */
    private PatchValidationResponse configMinMaxPidResponse(
            @NotNull Map<String, Long> pids, @NotNull PatchBook.Type type,
            @NotNull PatchRequest request) {
        String dataCloudVersion = request.getDataCloudVersion();
        PatchValidationResponse response = new PatchValidationResponse();
        response.setSuccess(true);
        response.setPatchBookType(type);
        response.setDataCloudVersion(dataCloudVersion);
        response.setMode(request.getMode());
        return response;
    }

    private LookupPatchResponse prepareResponse(@NotNull LookupPatchRequest request) {
        LookupPatchResponse response = new LookupPatchResponse();
        response.setPatchBookType(PatchBook.Type.Lookup);
        response.setDataCloudVersion(request.getDataCloudVersion());
        response.setMode(request.getMode());
        response.setLogLevel(request.getLogLevel());
        response.setDryRun(request.isDryRun());
        response.setStartAt(new Date());
        return response;
    }

    /*
     * 1. filter out entries that is end of life
     * 2. validate remaining entries
     * 3. configure response with input request and validation result
     */
    private PatchValidationResponse validate(
            @NotNull List<PatchBook> books, @NotNull PatchBook.Type type, @NotNull PatchRequest request) {
        String dataCloudVersion = request.getDataCloudVersion();

        Pair<Integer, List<PatchBookValidationError>> validationResult = patchBookValidator
                .validate(type, dataCloudVersion, books);
        Integer total = validationResult.getKey();
        List<PatchBookValidationError> errors = validationResult.getValue();
        Preconditions.checkNotNull(total);
        Preconditions.checkNotNull(errors);

        PatchValidationResponse response = new PatchValidationResponse();
        response.setSuccess(errors.isEmpty());
        response.setTotal(total);
        response.setValidationErrors(errors);
        response.setPatchBookType(type);
        response.setDataCloudVersion(dataCloudVersion);
        response.setMode(request.getMode());
        return response;
    }

    private List<PatchBook> load(@NotNull PatchMode mode, PatchBook.Type type, int offset,
            int limit, String sortByField) {
        // adding pagination parameters
        if (mode == PatchMode.Normal) {
            return patchBookEntityMgr.findByTypeWithPagination(offset, limit, sortByField, type);
        } else {
            // hot fix mode
            return patchBookEntityMgr.findByTypeAndHotFixWithPagination(offset, limit, sortByField,
                    type, true);
        }
    }

    private List<PatchBook> loadWithPagin(@NotNull PatchMode mode, PatchBook.Type type, int offset,
            int limit, Object pid) {
        // finding min and max Pid in patchBook table
        Map<String, Long> minMaxPid = patchBookEntityMgr.findMinMaxPid(type, PatchBook.COLUMN_PID);
        Long minPid = minMaxPid.get(MIN_PID);
        Long maxPid = minMaxPid.get(MAX_PID);
        // adding pagination parameters
        if (mode == PatchMode.Normal) {
            return patchBookEntityMgr.findByTypeWithPaginNoSort(minPid, maxPid, type);
        } else {
            // hot fix mode
            return patchBookEntityMgr.findByTypeAndHotFixWithPaginNoSort(minPid, maxPid, type,
                    true);
        }
    }

    private Map<String, Long> loadMinMaxPid(PatchBook.Type type, String pidColumn) {
        return patchBookEntityMgr.findMinMaxPid(type, pidColumn);
    }

    /*
     * case insensitive match
     */
    private PatchBook.Type getPatchBookType(String patchBookType) {
        return Arrays.stream(PatchBook.Type.values())
                .filter(e -> e.name().equalsIgnoreCase(patchBookType))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("Invalid patch book type"));
    }

    /*
     * if dataCloudVersion is provided in request, check if the version exist in DB
     * else use the current version
     */
    private void checkAndSetDataCloudVersion(@NotNull PatchRequest request) {
        if (StringUtils.isNotBlank(request.getDataCloudVersion())) {
            // check if the version exists
            String versionStr = request.getDataCloudVersion();
            DataCloudVersion version = dataCloudVersionEntityMgr.findVersion(versionStr);
            if (version == null) {
                String msg = String.format("Provided DataCloudVersion (%s) does not exist", versionStr);
                throw new IllegalArgumentException(msg);
            }
            return;
        }

        // use the current if not provided version
        DataCloudVersion version = dataCloudVersionEntityMgr.currentApprovedVersion();
        Preconditions.checkNotNull(version);
        request.setDataCloudVersion(version.getVersion());
    }

    private void checkRequired(PatchRequest request) {
        if (request == null || request.getMode() == null) {
            // TODO throw different error
            throw new IllegalArgumentException("Patch request missing required field");
        }
    }
}

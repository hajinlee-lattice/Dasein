package com.latticeengines.scoringinternalapi.controller;

import java.text.ParseException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "scoreinternal/score", description = "Internal REST resource for interacting with score API")
@RestController
@RequestMapping("score")
public class InternalScoreResource extends BaseScoring {

    @RequestMapping(value = "/models", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(HttpServletRequest request,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier,
            @RequestParam(value = "type", required = false) ModelType type) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        return getActiveModels(request, type, customerSpace);
    }

    @Deprecated
    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(HttpServletRequest request, @PathVariable ModelType type,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        return getActiveModels(request, type, customerSpace);
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(HttpServletRequest request, @PathVariable String modelId,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        return getModelFields(request, modelId, customerSpace);
    }

    @RequestMapping(value = "/modeldetails", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get paginated list of models for specified criteria")
    public List<ModelDetail> getPaginatedModels(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Should consider deleted models as well", required = false) @RequestParam(value = "considerDeleted", required = false, defaultValue = "false") boolean considerDeleted,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) throws ParseException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        if (!StringUtils.isEmpty(start) && start.contains("%2B")) {
            start = start.replace("%2B", "+");
        }
        return getPaginatedModels(request, start, offset, maximum, considerAllStatus, considerDeleted, customerSpace);
    }

    @RequestMapping(value = "/modeldetails/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of models for specified criteria")
    public int getModelCount(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Should consider deleted models as well", required = false) @RequestParam(value = "considerDeleted", required = false, defaultValue = "false") boolean considerDeleted,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) throws ParseException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        if (!StringUtils.isEmpty(start) && start.contains("%2B")) {
            start = start.replace("%2B", "+");
        }
        return getModelCount(request, start, considerAllStatus, considerDeleted, customerSpace);
    }

    @RequestMapping(value = "/record", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest, //
            @RequestParam(value = "tenantIdentifier", required = true) //
            String tenantIdentifier, //
            @RequestParam(value = "enrichInternalAttributes", required = false, defaultValue = "false") //
            boolean enrichInternalAttributes, //
            @RequestParam(value = "performFetchOnlyForMatching", required = false, defaultValue = "false") //
            boolean performFetchOnlyForMatching, //
            @RequestParam(value = "enableMatching", required = false, defaultValue = "true") //
            boolean enableMatching, //
            HttpServletResponse response) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        boolean forceSkipMatching = !enableMatching;
        return scorePercentileRecord(request, scoreRequest, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId, forceSkipMatching, true);
    }

    @RequestMapping(value = "/records", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request, //
            @RequestBody BulkRecordScoreRequest scoreRequest, //
            @RequestParam(value = "tenantIdentifier", required = true) //
            String tenantIdentifier, //
            @RequestParam(value = "enrichInternalAttributes", required = false, defaultValue = "false") //
            boolean enrichInternalAttributes, //
            @RequestParam(value = "performFetchOnlyForMatching", required = false, defaultValue = "false") //
            boolean performFetchOnlyForMatching, //
            @RequestParam(value = "enableMatching", required = false, defaultValue = "true") //
            boolean enableMatching) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        boolean forceSkipMatching = !enableMatching;
        return scorePercentileRecords(request, scoreRequest, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId, forceSkipMatching, true);
    }

    @RequestMapping(value = "/records/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scoreRecordsDebug(HttpServletRequest request, //
            @RequestBody BulkRecordScoreRequest scoreRequest, //
            @RequestParam(value = "tenantIdentifier", required = true) //
            String tenantIdentifier, //
            @RequestParam(value = "enrichInternalAttributes", required = false, defaultValue = "false") //
            boolean enrichInternalAttributes, //
            @RequestParam(value = "performFetchOnlyForMatching", required = false, defaultValue = "false") //
            boolean performFetchOnlyForMatching, //
            @RequestParam(value = "enableMatching", required = false, defaultValue = "true") //
            boolean enableMatching) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        boolean forceSkipMatching = !enableMatching;
        return scoreRecordsDebug(request, scoreRequest, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId, forceSkipMatching, true);
    }

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score a record including debug info such as probability")
    public DebugScoreResponse scoreProbabilityRecord(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest, //
            @RequestParam(value = "tenantIdentifier") //
            String tenantIdentifier, //
            @RequestParam(value = "enrichInternalAttributes", required = false, defaultValue = "false") //
            boolean enrichInternalAttributes, //
            @RequestParam(value = "performFetchOnlyForMatching", required = false, defaultValue = "false") //
            boolean performFetchOnlyForMatching, //
            @RequestParam(value = "enableMatching", required = false, defaultValue = "true") //
            boolean enableMatching) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        boolean forceSkipMatching = !enableMatching;
        return scoreProbabilityRecord(request, scoreRequest, customerSpace, enrichInternalAttributes,
                performFetchOnlyForMatching, requestId, forceSkipMatching, true);
    }

    @RequestMapping(value = "/record/apiconsole/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score a record including debug info such as probability via APIConsole")
    public DebugScoreResponse scoreAndEnrichRecordApiConsole(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest, //
            @RequestParam(value = "tenantIdentifier", required = true) //
            String tenantIdentifier, //
            @RequestParam(value = "enrichInternalAttributes", required = false, defaultValue = "false") //
            boolean enrichInternalAttributes, //
            @ApiParam(value = "Should enforce fuzzy match", //
                    required = false) //
            @RequestParam(value = "enforceFuzzyMatch", required = false, defaultValue = "true") //
            boolean enforceFuzzyMatch, //
            @ApiParam(value = "For fuzzy match, should skip DnB cache", //
                    required = false) //
            @RequestParam(value = "skipDnBCache", required = false, defaultValue = "false") //
            boolean skipDnBCache, //
            @RequestParam(value = "enableMatching", required = false, defaultValue = "true") //
            boolean enableMatching) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        setCustomerSpaceInMDC(customerSpace);
        String requestId = RequestIdUtils.getRequestIdentifierId(request);
        boolean forceSkipMatching = !enableMatching;
        return scoreAndEnrichRecordApiConsole(request, scoreRequest, customerSpace, enrichInternalAttributes, requestId,
                enforceFuzzyMatch, skipDnBCache, forceSkipMatching);
    }

    private void setCustomerSpaceInMDC(CustomerSpace customerSpace) {
        MDC.put("customerspace", CustomerSpace.shortenCustomerSpace(customerSpace.toString()));
    }

}

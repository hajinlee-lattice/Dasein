package com.latticeengines.scoringinternalapi.controller;

import java.text.ParseException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

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
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "scoreinternal/score", description = "Internal REST resource for interacting with score API")
@RestController
@RequestMapping("score")
public class InternalScoreResource extends BaseScoring {

    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(HttpServletRequest request, @PathVariable ModelType type,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        return getActiveModels(request, type, customerSpace);
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(HttpServletRequest request, @PathVariable String modelId,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
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
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) throws ParseException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        if (!StringUtils.isEmpty(start) && start.contains("%2B")) {
            start = start.replace("%2B", "+");
        }
        return getPaginatedModels(request, start, offset, maximum, considerAllStatus, customerSpace);
    }

    @RequestMapping(value = "/modeldetails/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of models for specified criteria")
    public int getModelCount(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) throws ParseException {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        if (!StringUtils.isEmpty(start) && start.contains("%2B")) {
            start = start.replace("%2B", "+");
        }
        return getModelCount(request, start, considerAllStatus, customerSpace);
    }

    @RequestMapping(value = "/record", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, @RequestBody ScoreRequest scoreRequest,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        return scorePercentileRecord(request, scoreRequest, customerSpace);
    }

    @RequestMapping(value = "/records", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request,
            @RequestBody BulkRecordScoreRequest scoreRequest,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        return scorePercentileRecords(request, scoreRequest, customerSpace);
    }

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score a record including debug info such as probability")
    public DebugScoreResponse scoreProbabilityRecord(HttpServletRequest request, @RequestBody ScoreRequest scoreRequest,
            @RequestParam(value = "tenantIdentifier", required = true) String tenantIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantIdentifier);
        return scoreProbabilityRecord(request, scoreRequest, customerSpace);
    }
}
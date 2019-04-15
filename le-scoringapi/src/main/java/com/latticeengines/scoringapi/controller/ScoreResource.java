package com.latticeengines.scoringapi.controller;

import java.text.ParseException;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.MDC;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.rest.RequestIdUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.scoringapi.exposed.ScoreUtils;
import com.latticeengines.scoringinternalapi.controller.BaseScoring;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "score", description = "REST resource for interacting with score API")
@RestController
@RequestMapping("")
public class ScoreResource extends BaseScoring {

    @Inject
    private BatonService batonService;

    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(HttpServletRequest request, @PathVariable ModelType type) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            return getActiveModels(request, type, customerSpace);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(HttpServletRequest request, @PathVariable String modelId) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            return getModelFields(request, modelId, customerSpace);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/modeldetails", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get paginated list of models for specified criteria")
    public List<ModelDetail> getPaginatedModels(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Should consider deleted models as well", required = false) @RequestParam(value = "considerDeleted", required = false, defaultValue = "false") boolean considerDeleted)
            throws ParseException {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            return getPaginatedModels(request, start, offset, maximum, considerAllStatus, considerDeleted,
                    customerSpace);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/modeldetails/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of models for specified criteria")
    public int getModelCount(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = false) @RequestParam(value = "start", required = false) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus,
            @ApiParam(value = "Should consider deleted models as well", required = false) @RequestParam(value = "considerDeleted", required = false, defaultValue = "false") boolean considerDeleted)
            throws ParseException {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            return getModelCount(request, start, considerAllStatus, considerDeleted, customerSpace);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/record", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            return scorePercentileRecord(request, scoreRequest, customerSpace, //
                    ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), //
                    false, requestId, false, false);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/records", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request, //
            @RequestBody BulkRecordScoreRequest scoreRequest) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            return scorePercentileRecords(request, scoreRequest, customerSpace, //
                    ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), //
                    false, requestId, false, false);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/records/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scoreRecordsDebug(HttpServletRequest request, //
            @RequestBody BulkRecordScoreRequest scoreRequest) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            return scoreRecordsDebug(request, scoreRequest, customerSpace, //
                    ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), false, requestId, false,
                    false);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score a record including debug info such as probability")
    public ScoreResponse scoreProbabilityRecord(HttpServletRequest request, //
            @RequestBody ScoreRequest scoreRequest) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        setCustomerSpaceInMDC(customerSpace);
        try {
            String requestId = RequestIdUtils.getRequestIdentifierId(request);
            return scoreProbabilityRecord(request, scoreRequest, customerSpace, //
                    ScoreUtils.canEnrichInternalAttributes(batonService, customerSpace), false, requestId, false,
                    false);
        } finally {
            removeCustomerSpaceFromMDC();
        }
    }

    private void setCustomerSpaceInMDC(CustomerSpace customerSpace) {
        MDC.put("customerspace", CustomerSpace.shortenCustomerSpace(customerSpace.toString()));
    }

    private void removeCustomerSpaceFromMDC() {
        MDC.remove("customerspace");
    }

}

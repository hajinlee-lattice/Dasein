package com.latticeengines.scoringapi.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.scoringapi.exposed.BulkRecordScoreRequest;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelDetail;
import com.latticeengines.scoringapi.exposed.Record;
import com.latticeengines.scoringapi.exposed.RecordScoreResponse;
import com.latticeengines.scoringapi.exposed.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.wordnik.swagger.annotations.ApiParam;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "score", description = "REST resource for interacting with score API")
@RestController
@RequestMapping("")
public class ScoreResource {

    private static final String MDC_CUSTOMERSPACE = "customerspace";

    private static final Log log = LogFactory.getLog(ScoreResource.class);

    private static final int MAX_ALLOWED_RECORDS = 200;

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    private ScoreRequestProcessor scoreRequestProcessor;

    @Autowired
    private RequestInfo requestInfo;

    @Autowired
    private Warnings warnings;

    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get active models")
    public List<Model> getActiveModels(HttpServletRequest request, @PathVariable ModelType type) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(type);
            List<Model> models = modelRetriever.getActiveModels(customerSpace, type);
            log.info(JsonUtils.serialize(models));
            return models;
        }
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(HttpServletRequest request, @PathVariable String modelId) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(modelId);
            Fields fields = modelRetriever.getModelFields(customerSpace, modelId);
            log.info(JsonUtils.serialize(fields));
            return fields;
        }
    }

    @RequestMapping(value = "/modeldetails", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get paginated list of models for specified criteria")
    public List<ModelDetail> getPaginatedModels(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = true) @RequestParam(value = "start", required = true) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus) {
        return fetchPaginatedModels(request, start, offset, maximum, considerAllStatus);
    }

    @RequestMapping(value = "/modeldetails/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of models for specified criteria")
    public int getModelCount(HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = true) @RequestParam(value = "start", required = true) String start,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus) {
        return fetchModelCount(request, start, considerAllStatus);
    }

    @RequestMapping(value = "/record", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, @RequestBody ScoreRequest scoreRequest) {
        return scoreRecord(request, scoreRequest, false);
    }

    @RequestMapping(value = "/records", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score list of records. Maximum " + MAX_ALLOWED_RECORDS
            + " records are allowed in a request.")
    public List<RecordScoreResponse> scorePercentileRecords(HttpServletRequest request,
            @RequestBody BulkRecordScoreRequest scoreRequest) {
        return scoreRecords(request, scoreRequest, false);
    }

    @RequestMapping(value = "/record/debug", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiIgnore
    @ApiOperation(value = "Score a record including debug info such as probability")
    public ScoreResponse scoreProbabilityRecord(HttpServletRequest request, @RequestBody ScoreRequest scoreRequest) {
        return scoreRecord(request, scoreRequest, true);
    }

    private ScoreResponse scoreRecord(HttpServletRequest request, ScoreRequest scoreRequest, boolean isDebug) {
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        requestInfo.put(RequestInfo.TENANT, customerSpace.toString());
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split("getTenantFromOAuth");
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequest));
            }
            ScoreResponse response = scoreRequestProcessor.process(customerSpace, scoreRequest, isDebug);
            if (warnings.hasWarnings()) {
                response.setWarnings(warnings.getWarnings());
                requestInfo.put("Warnings", JsonUtils.serialize(warnings.getWarnings()));
            }
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            requestInfo.put("HasWarning", String.valueOf(warnings.hasWarnings()));
            requestInfo.put("HasError", Boolean.toString(false));
            requestInfo.put("Score", String.valueOf(response.getScore()));
            requestInfo.logSummary();

            return response;
        }
    }

    // NOTE: This is simple implementation and has no optimization.
    // This change is just to make new rest endpoint functional and check it
    // in SVN. In future check-ins, optimizations will be done.
    private List<ModelDetail> fetchPaginatedModels(HttpServletRequest request, String start, int offset, int maximum,
            boolean considerAllStatus) {
        ModelType type = ModelType.ACCOUNT;
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(type);
            List<Model> models = modelRetriever.getActiveModels(customerSpace, type);
            List<ModelDetail> modelDetails = new ArrayList<>();

            for (Model model : models) {
                ModelDetail modelDetail = prepareModelDetail(customerSpace, model);
                modelDetails.add(modelDetail);
            }
            log.info(JsonUtils.serialize(models));
            return modelDetails;
        }
    }

    // NOTE: This is simple implementation and has no optimization.
    // This change is just to make new rest endpoint functional and check it
    // in SVN. In future check-ins, optimizations will be done.
    private int fetchModelCount(HttpServletRequest request, String start, boolean considerAllStatus) {
        ModelType type = ModelType.ACCOUNT;
        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            log.info(type);
            List<Model> models = modelRetriever.getActiveModels(customerSpace, type);
            return models.size();
        }
    }

    // NOTE: This is simple implementation and has no optimization.
    // This change is just to make new rest endpoint functional and check it
    // in SVN. In future check-ins, optimizations will be done.
    private List<RecordScoreResponse> scoreRecords(HttpServletRequest request, BulkRecordScoreRequest scoreRequests,
            boolean isDebug) {
        if (scoreRequests.getRecords().size() > MAX_ALLOWED_RECORDS) {
            throw new LedpException(LedpCode.LEDP_20027, //
                    new String[] { //
                            new Integer(MAX_ALLOWED_RECORDS).toString(),
                            new Integer(scoreRequests.getRecords().size()).toString() });
        }

        int idx = 0;
        List<RecordScoreResponse> responseList = new ArrayList<>();

        for (Record record : scoreRequests.getRecords()) {
            ScoreRequest scoreRequest = prepareScoreRecord(scoreRequests, idx, record.getAttributeValues(),
                    record.getModelIds().get(0));
            ScoreResponse response = scoreRecord(request, scoreRequest, isDebug);
            RecordScoreResponse scoreResponse = new RecordScoreResponse();
            scoreResponse.setEnrichmentAttributeValues(response.getEnrichmentAttributeValues());
            scoreResponse.setId(response.getId());
            scoreResponse.setLatticeId(response.getLatticeId());
            scoreResponse.setTimestamp(response.getTimestamp());
            scoreResponse.setWarnings(response.getWarnings());
            List<ScoreModelTuple> scoreModelTuples = new ArrayList<>();
            ScoreModelTuple scoreModelTuple = new ScoreModelTuple();
            scoreModelTuple.setModelId(scoreRequests.getRecords().get(idx).getModelIds().get(0));
            scoreModelTuple.setScore(response.getScore());
            scoreModelTuples.add(scoreModelTuple);
            scoreResponse.setScores(scoreModelTuples);
            responseList.add(scoreResponse);
            idx++;
        }

        return responseList;
    }

    private ScoreRequest prepareScoreRecord(BulkRecordScoreRequest scoreRequests, int idx, Map<String, Object> record,
            String modelId) {
        ScoreRequest scoreRequest = new ScoreRequest();
        scoreRequest.setModelId(modelId);
        scoreRequest.setRule(scoreRequest.getRule());
        scoreRequest.setSource(scoreRequests.getSource());
        scoreRequest.setRecord(record);
        return scoreRequest;
    }

    private ModelDetail prepareModelDetail(CustomerSpace customerSpace, Model model) {
        Fields fields = modelRetriever.getModelFields(customerSpace, model.getModelId());
        Long lastModifiedTimestamp = System.currentTimeMillis();
        ModelSummaryStatus status = ModelSummaryStatus.ACTIVE;
        ModelDetail modelDetail = new ModelDetail();
        modelDetail.setModel(model);
        modelDetail.setStatus(status);
        modelDetail.setFields(fields);
        modelDetail.setLastModifiedTimestamp(lastModifiedTimestamp);
        return modelDetail;
    }
}
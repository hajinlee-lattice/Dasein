package com.latticeengines.scoringapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringUtils;
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

import springfox.documentation.annotations.ApiIgnore;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warnings;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.context.RequestMetrics;
import com.latticeengines.scoringapi.exposed.context.SingleRecordMeasurement;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;
import com.wordnik.swagger.annotations.ApiParam;

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
    private MetricService metricService;

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
    public List<ModelDetail> getPaginatedModels(
            HttpServletRequest request,
            @ApiParam(value = "The UTC timestamp of last modification in ISO8601 format", required = true) @RequestParam(value = "start", required = true) String start,
            @ApiParam(value = "First record number from start", required = true) @RequestParam(value = "offset", required = true) int offset,
            @ApiParam(value = "Maximum records returned above offset", required = true) @RequestParam(value = "maximum", required = true) int maximum,
            @ApiParam(value = "Should consider models in any status or only in active status", required = true) @RequestParam(value = "considerAllStatus", required = true) boolean considerAllStatus) {
        return fetchPaginatedModels(request, start, offset, maximum, considerAllStatus);
    }

    @RequestMapping(value = "/modeldetails/count", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get total count of models for specified criteria")
    public int getModelCount(
            HttpServletRequest request,
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

            RequestMetrics metrics = generateMetrics(scoreRequest, response, customerSpace);
            SingleRecordMeasurement measurement = new SingleRecordMeasurement(metrics);
            metricService.write(MetricDB.SCORING, measurement);

            return response;
        }
    }

    private RequestMetrics generateMetrics(ScoreRequest scoreRequest, ScoreResponse response,
            CustomerSpace customerSpace) {
        RequestMetrics metrics = new RequestMetrics();
        metrics.setHasWarning(warnings.hasWarnings());
        metrics.setScore((int) response.getScore());
        metrics.setSource(StringUtils.trimToEmpty(scoreRequest.getSource()));
        metrics.setRule(StringUtils.trimToEmpty(scoreRequest.getRule()));
        metrics.setTenantId(customerSpace.toString());
        metrics.setModelId(scoreRequest.getModelId());

        Map<String, String> splits = httpStopWatch.getSplits();
        metrics.setGetTenantFromOAuthDurationMS(getSplit(splits, "getTenantFromOAuthDurationMS"));
        metrics.setMatchRecordDurationMS(getSplit(splits, "matchRecordDurationMS"));
        metrics.setParseRecordDurationMS(getSplit(splits, "parseRecordDurationMS"));
        metrics.setRequestDurationMS(getSplit(splits, "requestDurationMS"));
        metrics.setRequestPreparationDurationMS(getSplit(splits, "requestPreparationDurationMS"));
        metrics.setRetrieveModelArtifactsDurationMS(getSplit(splits, "retrieveModelArtifactsDurationMS"));
        metrics.setScoreRecordDurationMS(getSplit(splits, "scoreRecordDurationMS"));
        metrics.setTransformRecordDurationMS(getSplit(splits, "transformRecordDurationMS"));

        return metrics;
    }

    private int getSplit(Map<String, String> splits, String key) {
        return Integer.valueOf(splits.get(key));
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

    private List<RecordScoreResponse> scoreRecords(HttpServletRequest request, BulkRecordScoreRequest scoreRequests,
            boolean isDebug) {
        if (scoreRequests.getRecords().size() > MAX_ALLOWED_RECORDS) {
            throw new LedpException(LedpCode.LEDP_20027, //
                    new String[] { //
                    new Integer(MAX_ALLOWED_RECORDS).toString(),
                            new Integer(scoreRequests.getRecords().size()).toString() });
        }

        CustomerSpace customerSpace = OAuth2Utils.getCustomerSpace(request, oAuthUserEntityMgr);
        try (LogContext context = new LogContext(MDC_CUSTOMERSPACE, customerSpace)) {
            httpStopWatch.split("getTenantFromOAuth");
            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(scoreRequests));
            }
            List<RecordScoreResponse> response = scoreRequestProcessor.process(customerSpace, scoreRequests, isDebug);

            if (log.isInfoEnabled()) {
                log.info(JsonUtils.serialize(response));
            }

            return response;
        }
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
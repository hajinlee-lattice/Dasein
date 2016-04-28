package com.latticeengines.scoringapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import springfox.documentation.annotations.ApiIgnore;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.LogContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;
import com.latticeengines.scoringapi.exposed.warnings.Warnings;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

@Api(value = "score", description = "REST resource for interacting with score API")
@RestController
@RequestMapping("")
public class ScoreResource {

    private static final String MDC_CUSTOMERSPACE = "customerspace";

    private static final Log log = LogFactory.getLog(ScoreResource.class);

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

    @RequestMapping(value = "/record", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a record")
    public ScoreResponse scorePercentileRecord(HttpServletRequest request, @RequestBody ScoreRequest scoreRequest) {
        return scoreRecord(request, scoreRequest, false);
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

}
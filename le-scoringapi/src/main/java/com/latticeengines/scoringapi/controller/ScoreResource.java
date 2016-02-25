package com.latticeengines.scoringapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.DetailedErrors;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.scoringapi.exposed.AccountScoreRequest;
import com.latticeengines.scoringapi.exposed.ContactScoreRequest;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.Model;
import com.latticeengines.scoringapi.exposed.ModelType;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.model.ModelRetriever;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

@Api(value = "score", description = "REST resource for interacting with score API")
@RestController
@RequestMapping("")
@DetailedErrors
public class ScoreResource {

    @Autowired
    private ApplicationContext applicationContext;

    @PostConstruct
    public void status() throws Exception {
        String[] beanNames = applicationContext.getBeanDefinitionNames();

        for (String beanName : beanNames) {

            System.out.println("BN bean " + beanName + " : " + applicationContext.getBean(beanName).getClass().toString());
        }
    }

    private static final Log log = LogFactory.getLog(ScoreResource.class);

    @Autowired
    private ScoreResourceMockData scoreResourceMockData;

    @Autowired
    private ModelRetriever modelRetriever;

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    private ScoreRequestProcessor scoreRequestProcessor;

    @RequestMapping(value = "/models/{type}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get active models (only contact type is supported in M1)")
    public List<Model> getActiveModels(HttpServletRequest request, @PathVariable ModelType type) {
//        String tenantId = OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
        String tenantId = null;
        List<Model> models = modelRetriever.getActiveModels(tenantId, type);

        return scoreResourceMockData.activeModelMap.get(type);
    }

    @RequestMapping(value = "/models/{modelId}/fields", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Get fields for a model")
    public Fields getModelFields(@PathVariable String modelId) {
        Fields fields = modelRetriever.getModelFields(modelId);

        return scoreResourceMockData.modelFields.get(modelId);
    }

    @RequestMapping(value = "/accounts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score an account (not supported in M1)")
    public ScoreResponse scoreAccount(@RequestBody AccountScoreRequest request) {
        return scoreResourceMockData.simulateScore();
    }

    @RequestMapping(value = "/contacts", method = RequestMethod.POST, headers = "Accept=application/json")
    @ApiOperation(value = "Score a contact")
    public ScoreResponse scoreContact(@RequestBody ContactScoreRequest request) {
        ScoreResponse response = scoreRequestProcessor.process(request);

        return scoreResourceMockData.simulateScore();
    }




}
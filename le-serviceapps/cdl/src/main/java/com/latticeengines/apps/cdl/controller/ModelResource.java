package com.latticeengines.apps.cdl.controller;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.RatingEngineImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdlModel", description = "REST resource for rating engine")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/models")
public class ModelResource {

    private static final Logger log = LoggerFactory.getLogger(ModelResource.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Get all notes for single rating engine via rating engine id.")
    public String modelByParameters(@PathVariable String customerSpace, @PathVariable String modelName,
            @RequestBody RatingEngineModelingParameters ratingEngineModelingParameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        internalResourceProxy.setModelSummaryDownloadFlag(customerSpace);
        log.info(String.format("Rating Engine model endpoint called with parameters %s",
                ratingEngineModelingParameters.toString()));
        return ratingEngineImportMatchAndModelWorkflowSubmitter.submit(ratingEngineModelingParameters).toString();
    }
}

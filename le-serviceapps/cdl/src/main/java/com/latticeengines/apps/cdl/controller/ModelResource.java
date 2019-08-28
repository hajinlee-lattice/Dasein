package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CdlModelMetadataService;
import com.latticeengines.apps.cdl.workflow.CrossSellImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.CustomEventModelingWorkflowSubmitter;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.proxy.exposed.lp.ModelMetadataProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdlModel", description = "REST resource for rating engine")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/models")
public class ModelResource {

    private static final Logger log = LoggerFactory.getLogger(ModelResource.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Autowired
    private CdlModelMetadataService cdlModelMetadataService;

    @Inject
    private CrossSellImportMatchAndModelWorkflowSubmitter crossSellImportMatchAndModelWorkflowSubmitter;

    @Inject
    private CustomEventModelingWorkflowSubmitter customEventModelingWorkflowSubmitter;

    @Inject
    private ModelMetadataProxy modelMetadataProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public String model(@PathVariable String customerSpace, //
            @PathVariable String modelName, //
            @RequestBody ModelingParameters parameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryProxy.setDownloadFlag(customerSpace);
        log.info(String.format("model called with parameters %s", parameters.toString()));
        return customEventModelingWorkflowSubmitter.submit(customerSpace, parameters).toString();
    }

    @RequestMapping(value = "/rating/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Kick off a modeling job given the CrossSellModelingParameters")
    public String modelByParameters(@PathVariable String customerSpace, @PathVariable String modelName,
            @RequestBody CrossSellModelingParameters crossSellModelingParameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryProxy.setDownloadFlag(customerSpace);
        log.debug(String.format("Rating Engine model endpoint called with parameters %s",
                crossSellModelingParameters.toString()));
        return crossSellImportMatchAndModelWorkflowSubmitter.submit(crossSellModelingParameters).toString();
    }
}

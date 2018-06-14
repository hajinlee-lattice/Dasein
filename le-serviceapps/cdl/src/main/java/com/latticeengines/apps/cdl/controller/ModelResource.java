package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
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
import com.latticeengines.apps.cdl.workflow.CustomEventModelingWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RatingEngineImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.RatingEngineMatchAndModelWorkflowSubmitter;
import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.RatingEngineModelingParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.proxy.exposed.lp.ModelMetadataProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
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

    @Autowired
    private RatingEngineMatchAndModelWorkflowSubmitter ratingEngineModelWorkflowSubmitter;

    private InternalResourceRestApiProxy internalResourceProxy;

    @Autowired
    private CdlModelMetadataService cdlModelMetadataService;

    @Inject
    private RatingEngineImportMatchAndModelWorkflowSubmitter ratingEngineImportMatchAndModelWorkflowSubmitter;

    @Inject
    private CustomEventModelingWorkflowSubmitter customEventModelingWorkflowSubmitter;

    @Inject
    private ModelMetadataProxy modelMetadataProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

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
    @ApiOperation(value = "Kick off a modeling job given the RatingEngineModelingParameters")
    public String modelByParameters(@PathVariable String customerSpace, @PathVariable String modelName,
            @RequestBody RatingEngineModelingParameters ratingEngineModelingParameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        modelSummaryProxy.setDownloadFlag(customerSpace);
        log.info(String.format("Rating Engine model endpoint called with parameters %s",
                ratingEngineModelingParameters.toString()));
        return ratingEngineImportMatchAndModelWorkflowSubmitter.submit(ratingEngineModelingParameters).toString();
    }

    @RequestMapping(value = "/rating/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Kick off a modeling job given the RatingEngineModelingParameters")
    public String cloneAndRemodel(@PathVariable String customerSpace, @PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {

        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        log.info(String.format("cloneAndRemodel called with parameters %s, dedupOption: %s", parameters.toString(),
                parameters.getDeduplicationType()));
        ModelSummary modelSummary = internalResourceProxy
                .getModelSummaryFromModelId(parameters.getSourceModelSummaryId(), CustomerSpace.parse(customerSpace));

        List<Table> trainingTargetTables = cdlModelMetadataService.cloneTrainingTargetTable(modelSummary);
        List<String> trainingTargetTableNames = Arrays.asList(trainingTargetTables.get(0).getName(),
                trainingTargetTables.get(1).getName());

        Table eventTable = metadataProxy.getTable(customerSpace, modelSummary.getEventTableName());
        List<Attribute> userRefinedAttributes = modelMetadataProxy.getAttributesFromFields(customerSpace,
                eventTable.getAttributes(), parameters.getAttributes());
        modelSummaryProxy.setDownloadFlag(customerSpace);
        return ratingEngineModelWorkflowSubmitter
                .submit(trainingTargetTableNames, parameters, userRefinedAttributes, modelSummary).toString();

    }
}

package com.latticeengines.pls.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.NameValidationUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelMetadataService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.workflow.ImportMatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.MatchAndModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.PMMLModelWorkflowSubmitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "models", description = "REST resource for interacting with modeling workflows")
@RestController
@RequestMapping("/models")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ModelResource {
    private static final Logger log = Logger.getLogger(ModelResource.class);

    @Autowired
    private ImportMatchAndModelWorkflowSubmitter importMatchAndModelWorkflowSubmitter;

    @Autowired
    private MatchAndModelWorkflowSubmitter modelWorkflowSubmitter;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private ModelMetadataService modelMetadataService;

    @Autowired
    private PMMLModelWorkflowSubmitter pmmlModelWorkflowSubmitter;

    @Autowired
    private ModelCopyService modelCopyService;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, //
            @RequestBody ModelingParameters parameters) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        log.info(String.format("model called with parameters %s", parameters.toString()));
        return ResponseDocument.successResponse( //
                importMatchAndModelWorkflowSubmitter.submit(parameters).toString());

    }

    @RequestMapping(value = "/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    public ResponseDocument<String> cloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {
        log.info(String.format("cloneAndRemodel called with parameters %s, dedupOption: %s", parameters.toString(),
                parameters.getDeduplicationType()));
        Table clone = modelMetadataService.cloneTrainingTable(parameters.getSourceModelSummaryId());
        List<Attribute> userRefinedAttributes = modelMetadataService.getAttributesFromFields(clone.getAttributes(),
                parameters.getAttributes());
        ModelSummary modelSummary = modelSummaryService.getModelSummaryEnrichedByDetails(parameters
                .getSourceModelSummaryId());
        return ResponseDocument.successResponse( //
                modelWorkflowSubmitter.submit(clone.getName(), parameters, userRefinedAttributes, modelSummary)
                        .toString());
    }

    @RequestMapping(value = "/pmml/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a PMML model from the supplied module. Returns the job id.")
    public ResponseDocument<String> modelForPmml(@PathVariable String modelName, //
            @RequestParam(value = "module") String moduleName, //
            @RequestParam(value = "pivotfile") String pivotFileName, //
            @RequestParam(value = "pmmlfile") String pmmlFileName) {
        if (!NameValidationUtils.validateModelName(modelName)) {
            String message = String.format("Not qualified modelName %s contains unsupported characters.", modelName);
            log.error(message);
            throw new RuntimeException(message);
        }
        String appId = pmmlModelWorkflowSubmitter.submit(modelName, moduleName, pmmlFileName, pivotFileName).toString();
        return ResponseDocument.successResponse(appId);

    }

    @RequestMapping(value = "/copymodel/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Copy a model from current tenant to target tenant.")
    public ResponseDocument<Boolean> copyModel(@PathVariable String modelName,
            @RequestParam(value = "targetTenantId") String targetTenantId) {
        return ResponseDocument.successResponse( //
                modelCopyService.copyModel(targetTenantId, modelName));
    }

}

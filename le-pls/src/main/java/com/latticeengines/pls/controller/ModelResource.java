package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.workflow.CreateModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.ModelWorkflowSubmitter;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "models", description = "REST resource for interacting with modeling workflows")
@RestController
@RequestMapping("/models")
@PreAuthorize("hasRole('Edit_PLS_Data')")
public class ModelResource {
    private static final Logger log = Logger.getLogger(ModelResource.class);

    @Autowired
    private CreateModelWorkflowSubmitter createModelWorkflowSubmitter;

    @Autowired
    private ModelWorkflowSubmitter modelWorkflowSubmitter;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, @RequestBody ModelingParameters parameters) {
        try {
            return ResponseDocument.successResponse( //
                    createModelWorkflowSubmitter.submit(parameters).toString());
        } catch (Exception e) {
            log.error(String.format("Failure creating a model with name %s", parameters.getName()), e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/{modelName}/clone", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Clones and remodels with the specified model name.")
    public ResponseDocument<String> cloneAndRemodel(@PathVariable String modelName,
            @RequestBody CloneModelingParameters parameters) {
        try {
            String customerSpace = SecurityContextUtils.getCustomerSpace().toString();
            ModelSummary summary = modelSummaryEntityMgr.findValidByModelId(parameters.getSourceModelSummaryId());
            String eventTableName = summary.getEventTableName();
            Table eventTable = metadataProxy.getTable(customerSpace, eventTableName);
            if (eventTable == null) {
                throw new RuntimeException(String.format("Could not find event table with name %s", eventTableName));
            }
            Table clone = metadataProxy.cloneTable(customerSpace, eventTable.getName());
            clone.setAttributes(parameters.getAttributes());
            metadataProxy.updateTable(customerSpace, clone.getName(), clone);

            return ResponseDocument.successResponse( //
                    modelWorkflowSubmitter.submit(clone.getName(), parameters.getName()).toString());
        } catch (Exception e) {
            log.error(String.format("Failure creating a clone model with name %s", parameters.getName()), e);
            return ResponseDocument.failedResponse(e);
        }
    }

}

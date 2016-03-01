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
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.pls.workflow.CreateModelWorkflowSubmitter;
import com.latticeengines.pls.workflow.ModelWorkflowSubmitter;
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

    @RequestMapping(value = "/{modelName}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@PathVariable String modelName, @RequestBody ModelingParameters parameters) {
        try {
            if (parameters.getFilename() != null) {
                return ResponseDocument.successResponse( //
                        createModelWorkflowSubmitter.submit(parameters).toString());
            } else {
                return ResponseDocument.successResponse( //
                        modelWorkflowSubmitter.submit(parameters).toString());
            }

        } catch (Exception e) {
            log.error(e);
            return ResponseDocument.failedResponse(e);
        }
    }
}

package com.latticeengines.pls.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "modeling", description = "REST resource for interacting with modeling workflows")
@RestController
@RequestMapping("/modeling")
@PreAuthorize("hasRole('Edit_PLS_Models')")
public class ModelingResource {

    @RequestMapping(value = "/validations/{filename}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Run a validation workflow on the provided uploaded file. This will generate a report. Returns the job id.")
    public ResponseDocument<String> validateFile(@RequestBody SourceFile file) {
        // TODO
        return new ResponseDocument<>("LosLobosKickYourAss");
    }

    @RequestMapping(value = "/models/{filename}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Generate a model from the supplied file and parameters. Returns the job id.")
    public ResponseDocument<String> model(@RequestParam("filename") String filename,
            @RequestBody ModelingParameters parameters) {
        // TODO
        return new ResponseDocument<>("LosLobosKickYourFace");
    }

}

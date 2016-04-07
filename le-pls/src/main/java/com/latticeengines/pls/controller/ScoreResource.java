package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.ScoringJobService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scores", description = "REST resource for interacting with score workflows")
@RestController
@RequestMapping("/scores")
public class ScoreResource {
    // TODO rights

    @Autowired
    private ScoringJobService scoringJobService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the provided testing set file. Returns the job id.")
    public String score( //
            @PathVariable String modelId, //
            @RequestParam(value = "fileName") String fileName) {
        // TODO implement
        return null;
    }

    @RequestMapping(value = "/{modelId}/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the training data for the provided model.")
    public String scoreTrainingData(@PathVariable String modelId) {
        return scoringJobService.scoreTrainingData(modelId);
    }
}

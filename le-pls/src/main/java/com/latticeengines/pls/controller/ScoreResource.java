package com.latticeengines.pls.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.pls.service.ScoringJobService;

@Api(value = "scores", description = "REST resource for interacting with score workflows")
@RestController
@RequestMapping("/scores")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoreResource {

    private static final Logger log = Logger.getLogger(ScoreResource.class);

    @Autowired
    private ScoringJobService scoringJobService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the provided testing set file. Returns the job id.")
    public String score( //
            @PathVariable String modelId, //
            @RequestParam(value = "fileName") String fileName, //
            @RequestParam(value = "useRtsApi", required = false) Boolean useRtsApi) {
        log.info(String.format("Scoring testing file for model %s (useRtsApi=%s)", modelId, useRtsApi));
        return JsonUtils.serialize(ImmutableMap.<String, String> of("applicationId", //
                scoringJobService.scoreTestingData(modelId, fileName, useRtsApi)));

    }

    @RequestMapping(value = "/{modelId}/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the training data for the provided model.")
    public String scoreTrainingData(//
            @PathVariable String modelId, //
            @RequestParam(value = "useRtsApi", required = false) Boolean useRtsApi) {
        log.info(String.format("Scoring training file for model %s (useRtsApi=%s)", modelId, useRtsApi));
        return JsonUtils.serialize(ImmutableMap.<String, String> of("applicationId", //
                scoringJobService.scoreTrainingData(modelId, useRtsApi)));
    }
}

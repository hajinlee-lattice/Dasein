package com.latticeengines.pls.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.pls.service.ScoringJobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scores", description = "REST resource for interacting with score workflows")
@RestController
@RequestMapping("/scores")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoreResource {

    private static final Logger log = LoggerFactory.getLogger(ScoreResource.class);

    @Autowired
    private ScoringJobService scoringJobService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the provided testing set file. Returns the job id.")
    public String score(//
            @PathVariable String modelId, //
            @RequestParam(value = "fileName") String fileName, //
            @RequestParam(value = "performEnrichment", required = false) Boolean performEnrichment, //
            @RequestParam(value = "useRtsApi", required = false) Boolean useRtsApi, //
            @RequestParam(value = "debug", required = false) Boolean debug) {
        log.info(String.format("Scoring testing file for model %s (performEnrichment=%s,useRtsApi=%s)", modelId,
                performEnrichment, useRtsApi));
        return JsonUtils.serialize(ImmutableMap.<String, String> of("applicationId", //
                scoringJobService.scoreTestingData(modelId, fileName, performEnrichment, debug)));

    }

    @RequestMapping(value = "/{modelId}/training", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the training data for the provided model.")
    public String scoreTrainingData(//
            @PathVariable String modelId, //
            @RequestParam(value = "performEnrichment", required = false) Boolean performEnrichment, //
            @RequestParam(value = "useRtsApi", required = false) Boolean useRtsApi, //
            @RequestParam(value = "debug", required = false) Boolean debug) {
        log.info(String.format("Scoring training file for model %s (performEnrichment=%s,useRtsApi=%s)", modelId,
                performEnrichment, useRtsApi));
        return JsonUtils.serialize(ImmutableMap.<String, String> of("applicationId", //
                scoringJobService.scoreTrainingData(modelId, performEnrichment, debug)));
    }

    @RequestMapping(value = "/rating/{modelId}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Score the provided query. Returns the job id.")
    public String scoreRating(//
            @PathVariable String modelId, //
            @RequestParam(value = "displayName") String displayName, //
            @RequestParam(value = "tableToScoreName", required = false) String tableToScoreName, //
            @RequestBody(required = false) EventFrontEndQuery targetQuery) {
        return JsonUtils.serialize(ImmutableMap.<String, String> of("applicationId", //
                scoringJobService.scoreRatinggData(modelId, displayName, targetQuery, tableToScoreName)));

    }
}

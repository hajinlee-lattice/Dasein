package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.service.ScoringJobService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scores/jobs", description = "REST resource for retrieving job information for score operations")
@RestController
@RequestMapping("/scores/jobs")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringJobResource {

    @Autowired
    private ScoringJobService scoringJobService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all scoring jobs for the provided model id")
    public List<Job> findAll(@PathVariable String modelId) {
        return scoringJobService.getJobs(modelId);
    }

    @RequestMapping(value = "{applicationId}/results", method = RequestMethod.GET, produces = "application/csv")
    @ResponseBody
    @ApiOperation(value = "Retrieve results csv for the provided applicationId")
    public void getResultsCsv(@PathVariable String applicationId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getResults(applicationId);
            response.setContentType("application/csv");
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", "results.csv"));
            IOUtils.copy(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18102, e);
        }
    }
}

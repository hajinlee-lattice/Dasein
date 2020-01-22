package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.util.GzipUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.service.ScoringJobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scores/jobs", description = "REST resource for retrieving job information for score operations")
@RestController
@RequestMapping("/scores/jobs")
@PreAuthorize("hasRole('View_PLS_Data')")
public class ScoringJobResource {

    @Inject
    private ScoringJobService scoringJobService;

    @RequestMapping(value = "/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all scoring jobs for the provided model id")
    public List<Job> findAll(@PathVariable String modelId) {
        return scoringJobService.getJobs(modelId);
    }

    @RequestMapping(value = "{jobId}/results/score", method = RequestMethod.GET, produces = "application/csv")
    @ResponseBody
    @ApiOperation(value = "Retrieve results csv for the provided jobId")
    public void getResultsCsv(@PathVariable String jobId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getScoreResults(jobId);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Disposition",
                    String.format("attachment; filename=\"%s\"", scoringJobService.getResultScoreFileName(jobId)));
            GzipUtils.copyAndCompressStream(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18102, e);
        }
    }

    @RequestMapping(value = "{jobId}/results/pivotscore", method = RequestMethod.GET, produces = "application/csv")
    @ResponseBody
    @ApiOperation(value = "Retrieve results csv for the provided jobId")
    public void getPivotScoringResultCsv(@PathVariable String jobId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getPivotScoringFile(jobId);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Disposition",
                    String.format("attachment; filename=\"%s\"", scoringJobService.getResultPivotScoreFileName(jobId)));
            GzipUtils.copyAndCompressStream(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18102, e);
        }
    }

    @RequestMapping(value = "{jobId}/errors", method = RequestMethod.GET, produces = "application/csv")
    @ResponseBody
    @ApiOperation(value = "Retrieve file import errors")
    public void getImportErrors(@PathVariable String jobId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getScoringErrorStream(jobId);
            response.setContentType("application/csv");
            response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", "errors.csv"));
            IOUtils.copy(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18093, e);
        }
    }
}

package com.latticeengines.pls.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.springframework.http.MediaType;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
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

    @GetMapping("/{modelId}")
    @ResponseBody
    @ApiOperation(value = "Retrieve all scoring jobs for the provided model id")
    public List<Job> findAll(@PathVariable String modelId) {
        return scoringJobService.getJobs(modelId);
    }

    @GetMapping(value = "{jobId}/results/score", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    @ApiOperation(value = "Retrieve results csv for the provided jobId")
    public void getResultsCsv(@PathVariable String jobId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getScoreResults(jobId);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Disposition",
                    String.format("attachment; filename=\"%s\"", scoringJobService.getResultScoreFileName(jobId)));
            GzipUtils.copyAndCompressStream(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18102, e);
        }
    }

    @GetMapping(value = "{jobId}/results/pivotscore", produces = MediaType.TEXT_PLAIN_VALUE)
    @ResponseBody
    @ApiOperation(value = "Retrieve results csv for the provided jobId")
    public void getPivotScoringResultCsv(@PathVariable String jobId, HttpServletResponse response) {
        try {
            InputStream is = scoringJobService.getPivotScoringFile(jobId);
            response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE);
            response.setHeader("Content-Encoding", "gzip");
            response.setHeader("Content-Disposition",
                    String.format("attachment; filename=\"%s\"", scoringJobService.getResultPivotScoreFileName(jobId)));
            GzipUtils.copyAndCompressStream(is, response.getOutputStream());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18102, e);
        }
    }

    @GetMapping(value = "{jobId}/errors", produces = MediaType.TEXT_PLAIN_VALUE)
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

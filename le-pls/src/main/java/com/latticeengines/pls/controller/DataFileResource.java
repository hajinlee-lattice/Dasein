package com.latticeengines.pls.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.DataFileProviderService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/datafiles")
@PreAuthorize("hasRole('View_PLS_Configurations')")
public class DataFileResource {

    @Autowired
    private DataFileProviderService dataFileProviderService;

    @RequestMapping(value = "/modeljson", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model json file for specific model summary")
    public void getModelJsonFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_JSON, "modelsummary.json");
    }

    @RequestMapping(value = "/diagnosticsjson", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostics json file for specific model summary")
    public void getDiagnosticsJsonFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_JSON, "diagnostics.json");
    }

    @RequestMapping(value = "/metadataavsc", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get metadata avsc file for specific model summary")
    public void getMetadataAvscFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_JSON, "metadata.avsc");
    }

    @RequestMapping(value = "/predictorcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top predictors csv file for specific model summary")
    public void getTopPredictorsCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_model.csv");
    }

    @RequestMapping(value = "/readoutcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get readout sample csv file for specific model summary")
    public void getReadoutSampleCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_readoutsample.csv");
    }

    @RequestMapping(value = "/scorecsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get score csv file for specific model summary")
    public void getScoreCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.TEXT_PLAIN, ".*_scored.txt");
    }

    @RequestMapping(value = "/explorercsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get threshold explorer csv file for specific model summary")
    public void getThresholdExplorerCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_explorer.csv");
    }

    @RequestMapping(value = "/rfmodelcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get RF model csv file for specific model summary")
    public void getRfModelCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.TEXT_PLAIN, ".*rf_model.txt");
    }

    @RequestMapping(value = "/postmatcheventtablecsv/{eventTableType}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Post Match Event Table csv file for specific model summary")
    public void getPostMatchTrainingEventTableCsvFile(@RequestParam(value = "modelId") String modelId,
            @PathVariable String eventTableType, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        response.setHeader("Content-Encoding", "gzip");
        if (eventTableType.equalsIgnoreCase("training")) {
            dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                    "postMatchEventTable.*Training.*.csv");
        } else if (eventTableType.equalsIgnoreCase("test")) {
            dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                    "postMatchEventTable.*Test.*.csv");
        }
    }
}

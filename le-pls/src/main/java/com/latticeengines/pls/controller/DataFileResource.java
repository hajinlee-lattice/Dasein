package com.latticeengines.pls.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.pls.service.DataFileProviderService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/datafiles")
@PreAuthorize("hasRole('View_PLS_Configuration')")
public class DataFileResource {

    @Autowired
    private DataFileProviderService dataFileProviderService;

    @RequestMapping(value = "/modeljson/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model json file for specific model summary")
    public void getModelJsonFile(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/json", "modelsummary.json");
    }
    
    @RequestMapping(value = "/diagnosticsjson/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostics json file for specific model summary")
    public void getDiagnosticsJsonFile(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/json", "diagnostics.json");
    }

    @RequestMapping(value = "/metadataavsc/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get metadata avsc file for specific model summary")
    public void getMetadataAvscFile(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/json", "metadata.avsc");
    }

    @RequestMapping(value = "/predictorcsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get top predictors csv file for specific model summary")
    public void getTopPredictorsCsvFile(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_model.csv");
    }

    @RequestMapping(value = "/readoutcsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get readout sample csv file for specific model summary")
    public void getReadoutSampleCsvFile(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_readoutsample.csv");
    }

    @RequestMapping(value = "/scorecsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get score csv file for specific model summary")
    public void getScoreCsvFile(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "text/plain", ".*_scored.txt");
    }

    @RequestMapping(value = "/explorercsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get threshold explorer csv file for specific model summary")
    public void getThresholdExplorerCsvFile(@PathVariable String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_explorer.csv");
    }
    
    @RequestMapping(value = "/rfmodelcsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get RF model csv file for specific model summary")
    public void getRfModelCsvFile(@PathVariable String modelId, HttpServletRequest request, HttpServletResponse response)
            throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, "text/plain", ".*rf_model.txt");
    }

}

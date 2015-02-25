package com.latticeengines.pls.controller;

import javax.ws.rs.core.Response;

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

@Api(value = "datafiles", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/datafiles")
@PreAuthorize("View_PLS_Configuration")
public class DataFileResource {
    
    @Autowired
    private DataFileProviderService dataFileProviderService;

    @RequestMapping(value = "/modeljson/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json", produces = { "text/plain" })
    @ResponseBody
    @ApiOperation(value = "Get model json file for specific model summary")
    public Response getModelJsonFile(@PathVariable String modelId) {
        return null;
    }

    @RequestMapping(value = "/predictorcsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json", produces = { "text/plain" })
    @ResponseBody
    @ApiOperation(value = "Get top predictors csv file for specific model summary")
    public Response getTopPredictorsCsvFile(@PathVariable String modelId) {
        return null;
    }

    @RequestMapping(value = "/readoutcsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json", produces = { "text/plain" })
    @ResponseBody
    @ApiOperation(value = "Get readout sample csv file for specific model summary")
    public Response getReadoutSampleCsvFile(@PathVariable String modelId) {
        return null;
    }

    @RequestMapping(value = "/scorecsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json", produces = { "text/plain" })
    @ResponseBody
    @ApiOperation(value = "Get score csv file for specific model summary")
    public Response getScoreCsvFile(@PathVariable String modelId) {
        return null;
    }

    @RequestMapping(value = "/explorercsv/{modelId}", method = RequestMethod.GET, headers = "Accept=application/json", produces = { "text/plain" })
    @ResponseBody
    @ApiOperation(value = "Get threshold explorer csv file for specific model summary")
    public Response getThresholdExplorerCsvFile(@PathVariable String modelId) {
        return null;
    }
}

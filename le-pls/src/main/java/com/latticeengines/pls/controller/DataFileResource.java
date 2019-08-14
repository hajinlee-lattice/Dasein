package com.latticeengines.pls.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.app.exposed.download.HttpFileDownLoader;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.DataFileProviderService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafile", description = "REST resource for retrieving data files")
@RestController
@RequestMapping(value = "/datafiles")
@PreAuthorize("hasRole('View_PLS_Configurations')")
public class DataFileResource {

    private static final Logger log = LoggerFactory.getLogger(DataFileResource.class);

    @Autowired
    private DataFileProviderService dataFileProviderService;

    @RequestMapping(value = "/modeljson", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model json file for specific model summary")
    public void getModelJsonFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_JSON,
                "modelsummary.json");
    }

    @RequestMapping(value = "/diagnosticsjson", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get diagnostics json file for specific model summary")
    public void getDiagnosticsJsonFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

        dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_JSON,
                "diagnostics.json");
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

        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*_model.csv",
                HttpFileDownLoader.DownloadMode.TOP_PREDICTOR);
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
    public void getScoreCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {

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
    public void getRfModelCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        dataFileProviderService.downloadFile(request, response, modelId, "application/csv", ".*rf_model.txt",
                HttpFileDownLoader.DownloadMode.RF_MODEL);
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
                    "postMatchEventTable.*allTraining.*.csv");
        } else if (eventTableType.equalsIgnoreCase("exportrftrain")) {
            try {
                dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                        ".*exportrftrain.csv");
            } catch (Exception e) {
                log.warn("Final modeling file does not exist, fall back to previous modeling file!");
                dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                        "postMatchEventTable.*allTraining.*.csv");
            }
        } else if (eventTableType.equalsIgnoreCase("test")) {
            dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                    "postMatchEventTable.*allTest.*.csv");
        }
    }

    @RequestMapping(value = "/scoredeventtablecsv/{scoredfiletype}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get Scored Event Table csv file for specific model summary")
    public void getScoredEventTableCsvFile(@RequestParam(value = "modelId") String modelId,
            @PathVariable String scoredfiletype, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        response.setHeader("Content-Encoding", "gzip");
        if (scoredfiletype.equalsIgnoreCase("score")) {
            dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                    ".*_scored_.*.csv");
        } else if (scoredfiletype.equalsIgnoreCase("pivot")) {
            dataFileProviderService.downloadFile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM,
                    ".*_pivoted_.*.csv");
        }
    }

    @RequestMapping(value = "/pivotmappingcsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get pivot file for download")
    public void getPivotMappingCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        dataFileProviderService.downloadPivotFile(request, response, modelId, MediaType.TEXT_PLAIN);
    }

    @RequestMapping(value = "/trainingfilecsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get training set used for modeling")
    public void getTrainingSetCsvFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        response.setHeader("Content-Encoding", "gzip");
        dataFileProviderService.downloadTrainingSet(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM);
    }

    @RequestMapping(value = "/modelprofileavro", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get model profile avro file.")
    public void getModelProfileAvroFile(@RequestParam(value = "modelId") String modelId, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        response.setHeader("Content-Encoding", "gzip");
        dataFileProviderService.downloadModelProfile(request, response, modelId, MediaType.APPLICATION_OCTET_STREAM);
    }

    @RequestMapping(value = "/sourcefilecsv/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get source file uploaded to create model or score against model via Application Id")
    public void getSourceFileViaAppId( //
            @PathVariable String applicationId, //
            @RequestParam(value = "fileName", required = true) String fileName, //
            HttpServletRequest request, //
            HttpServletResponse response) throws IOException {
        try {
            dataFileProviderService.downloadFileByApplicationId(request, response, "application/csv", applicationId,
                    fileName);
        } catch (Exception e) {
            throw e;
        }
    }

    @RequestMapping(value = "/sourcefile", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get source file uploaded to create model or score against model via internal file name")
    public void getSourceFileViaFileName( //
            @RequestParam(value = "fileName", required = true) String fileName, //
            @RequestParam(value = "filePath", required = false) String filePath,
            @RequestParam(value = "bucketName", required = false) String bucketName,
            HttpServletRequest request, //
            HttpServletResponse response) throws IOException {
        try {
            if (StringUtils.isEmpty(filePath)) {
                dataFileProviderService.downloadFileByFileName(request, response, "application/csv", fileName);
            } else {
                dataFileProviderService.downloadS3File(request, response, "application/csv", fileName, filePath,
                        bucketName);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @RequestMapping(value = "/errorscsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get error file via file path")
    public void getErrorsCsvFile( //
            @RequestParam(value = "filePath", required = true) String filePath, //
            HttpServletRequest request, //
            HttpServletResponse response) throws IOException {
        try {
            dataFileProviderService.downloadFileByPath(request, response, "application/csv", filePath);
        } catch (Exception e) {
            throw e;
        }
    }

    @RequestMapping(value = "/bundlecsv", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get bundle file from s3")
    public void getBundleCsvFile(
            HttpServletRequest request, //
            HttpServletResponse response) throws Exception {
        String tenantId = MultiTenantContext.getShortTenantId();
        if (tenantId == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        try {
            dataFileProviderService.downloadCurrentBundleFile(request, response, "application/csv");
        } catch (Exception e) {
            log.error("Download csv Failed: " + e.getMessage());
            throw e;
        }
    }
}

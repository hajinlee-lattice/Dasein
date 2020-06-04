package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.dcp.service.UploadService;
import com.latticeengines.apps.dcp.workflow.DCPSourceImportWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadDiagnostics;
import com.latticeengines.domain.exposed.dcp.UploadStats;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Upload")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/uploads")
public class UploadResource {

    private static final Logger log = LoggerFactory.getLogger(UploadResource.class);

    @Inject
    private UploadService uploadService;

    @Inject
    private DCPSourceImportWorkflowSubmitter importSubmitter;

    @PostMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "create an upload")
    public UploadDetails createUpload(@PathVariable String customerSpace,
                                      @PathVariable String sourceId, @RequestBody UploadConfig uploadConfig) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (uploadConfig == null) {
            log.error("Create Upload with empty uploadConfig!");
            throw new RuntimeException("Cannot create upload with empty create upload config input!");
        }
        return uploadService.createUpload(customerSpace, sourceId, uploadConfig);
    }

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation(value = "get upload list")
    public List<UploadDetails> getUploads(@PathVariable String customerSpace, @PathVariable String sourceId,
                                  @RequestParam(value = "status", required = false) Upload.Status status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (status != null) {
            return uploadService.getUploads(customerSpace, sourceId, status);
        } else {
            return uploadService.getUploads(customerSpace, sourceId);
        }
    }

    @GetMapping("/uploadId/{uploadId}")
    @ResponseBody
    @ApiOperation(value = "Get upload record by uploadId")
    public UploadDetails getUploadByUploadId(@PathVariable String customerSpace, @PathVariable String uploadId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        log.info(String.format("Get upload for customer %s, with uploadId %s", customerSpace, uploadId));
        return uploadService.getUploadByUploadId(customerSpace, uploadId);
    }

    @PutMapping("/update/{uploadId}/matchResult/{tableName}")
    @ResponseBody
    @ApiOperation(value = "update the upload config")
    public void registerMatchResult(@PathVariable String customerSpace,
                                    @PathVariable String uploadId,
                                    @PathVariable String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.registerMatchResult(customerSpace, uploadId, tableName);
    }

    @PutMapping("/update/{uploadId}/config")
    @ResponseBody
    @ApiOperation(value = "update the upload config")
    public void updateConfig(@PathVariable String customerSpace,
                             @PathVariable String uploadId,
                             @RequestBody UploadConfig uploadConfig) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadConfig(customerSpace, uploadId, uploadConfig);
    }

    @PutMapping("/update/{uploadId}/status/{status}")
    @ResponseBody
    @ApiOperation(value = "update the upload status")
    public void updateStatus(@PathVariable String customerSpace,
                             @PathVariable String uploadId,
                             @PathVariable Upload.Status status,
                             @RequestBody(required = false) UploadDiagnostics uploadDiagnostics) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadStatus(customerSpace, uploadId, status, uploadDiagnostics);
    }

    @PutMapping("/{uploadId}/stats/{statsId}")
    @ResponseBody
    @ApiOperation(value = "Get upload record by pid")
    public void updateStatsContent(@PathVariable String customerSpace, @PathVariable String uploadId,
                                    @PathVariable Long statsId, @RequestBody UploadStats uploadStats) {
        uploadService.updateStatistics(uploadId, statsId, uploadStats);
    }

    @PutMapping("/{uploadId}/latest-stats/{statsId}")
    @ResponseBody
    @ApiOperation(value = "Get upload record by pid")
    public UploadDetails setLatestStats(@PathVariable String customerSpace, @PathVariable String uploadId, @PathVariable Long statsId) {
        return uploadService.setLatestStatistics(uploadId, statsId);
    }

    @PostMapping("/startimport")
    @ResponseBody
    @ApiOperation(value = "Invoke DCP import workflow. Returns the job id.")
    public String startImport(@PathVariable String customerSpace, @RequestBody DCPImportRequest request) {
        ApplicationId appId = importSubmitter.submit(CustomerSpace.parse(customerSpace), request,
                new WorkflowPidWrapper(-1L));
        return appId.toString();
    }
}

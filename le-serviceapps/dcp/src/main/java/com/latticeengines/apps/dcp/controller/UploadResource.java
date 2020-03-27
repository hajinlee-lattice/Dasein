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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Upload", description = "REST resource for upload")
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
    public Upload createUpload(@PathVariable String customerSpace,
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
    public List<Upload> getUploads(@PathVariable String customerSpace, @PathVariable String sourceId,
                                  @RequestParam(value = "status", required = false) Upload.Status status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (status != null) {
            return uploadService.getUploads(customerSpace, sourceId, status);
        } else {
            return uploadService.getUploads(customerSpace, sourceId);
        }
    }

    @GetMapping("/{pid}")
    @ResponseBody
    @ApiOperation(value = "Get upload record by pid")
    private Upload getUploadByPid(@PathVariable String customerSpace, @PathVariable Long pid) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        log.info(String.format("Get upload for customer %s, with pid %d", customerSpace, pid));
        return uploadService.getUpload(customerSpace, pid);
    }

    @PutMapping("/update/{uploadPid}/config")
    @ResponseBody
    @ApiOperation(value = "update the upload config")
    public void updateConfig(@PathVariable String customerSpace,
                             @PathVariable Long uploadPid,
                             @RequestBody UploadConfig uploadConfig) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadConfig(customerSpace, uploadPid, uploadConfig);
    }

    @PutMapping("/update/{uploadPid}/status/{status}")
    @ResponseBody
    @ApiOperation(value = "update the upload status")
    public void updateStatus(@PathVariable String customerSpace,
                             @PathVariable Long uploadPid,
                             @PathVariable Upload.Status status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadStatus(customerSpace, uploadPid, status);
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

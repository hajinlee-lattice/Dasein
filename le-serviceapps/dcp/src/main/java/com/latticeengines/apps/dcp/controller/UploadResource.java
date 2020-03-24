package com.latticeengines.apps.dcp.controller;

import java.util.List;

import javax.inject.Inject;

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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Upload", description = "REST resource for upload")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/upload")
public class UploadResource {

    @Inject
    private UploadService uploadService;

    private static final Logger log = LoggerFactory.getLogger(UploadResource.class);

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


    @PutMapping("/uploadId/{uploadId}/config")
    @ResponseBody
    @ApiOperation(value = "update the upload config")
    public void updateConfig(@PathVariable String customerSpace,
                             @PathVariable Long uploadId,
                             @RequestBody UploadConfig uploadConfig) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadConfig(customerSpace, uploadId, uploadConfig);
    }

    @PutMapping("/uploadId/{uploadId}/status/{status}")
    @ResponseBody
    @ApiOperation(value = "update the upload status")
    public void updateStatus(@PathVariable String customerSpace,
                             @PathVariable Long uploadId,
                             @PathVariable Upload.Status status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        uploadService.updateUploadStatus(customerSpace, uploadId, status);
    }
}

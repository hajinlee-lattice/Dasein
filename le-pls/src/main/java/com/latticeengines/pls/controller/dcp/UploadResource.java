package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;
import com.latticeengines.domain.exposed.dcp.UploadJobDetails;
import com.latticeengines.domain.exposed.exception.UIActionUtils;
import com.latticeengines.pls.service.dcp.UploadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Uploads")
@RestController
@RequestMapping("/uploads")
@PreAuthorize("hasRole('View_DCP_Projects')")
public class UploadResource {

    @Inject
    private UploadService uploadService;

    @GetMapping("/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get uploads by sourceId")
    public List<UploadDetails> getAllBySourceId(@PathVariable String sourceId, @RequestParam(required = false) Upload.Status status,
                                                @RequestParam(required = false, defaultValue = "false") Boolean includeConfig,
                                                @RequestParam(required = false, defaultValue = "1") int pageIndex,
                                                @RequestParam(required = false, defaultValue = "20") int pageSize) {
        return uploadService.getAllBySourceId(sourceId, status, includeConfig, pageIndex, pageSize);
    }

    @GetMapping("/uploadId/{uploadId}")
    @ResponseBody
    @ApiOperation("Get upload by uploadID")
    public UploadDetails getUpload(@PathVariable String uploadId,
                                   @RequestParam(required = false, defaultValue = "false") Boolean includeConfig) {
        if (uploadId == null) {
            return null;
        } else {
            return uploadService.getByUploadId(uploadId, includeConfig);
        }
    }

    @GetMapping("/uploadId/{uploadId}/token")
    @ResponseBody
    @ApiOperation("Generate a token for downloading zip file of the upload results")
    public String getToken(@PathVariable String uploadId, @RequestParam(required = false) List<UploadFileDownloadConfig.FileType> files) {
        return uploadService.generateToken(uploadId, files);
    }

    @PostMapping("/startimport")
    @ResponseBody
    @ApiOperation(value = "Invoke DCP import workflow. Returns the upload details.")
    public UploadDetails startImport(@RequestBody DCPImportRequest importRequest) {
        importRequest.setUserId(MultiTenantContext.getEmailAddress());
        try {
            return uploadService.startImport(importRequest);
        } catch (Exception e) {
            throw UIActionUtils.handleException(e);
        }
    }

    @GetMapping("/uploadId/{uploadId}/jobDetails")
    @ResponseBody
    @ApiOperation(value = "Get upload job details by uploadId")
    public UploadJobDetails getJobDetailsByUploadId(@PathVariable String uploadId) {
        if (uploadId == null) {
            return null;
        } else {
            return uploadService.getJobDetailsByUploadId(uploadId);
        }
    }
}

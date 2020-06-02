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

import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
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

    @GetMapping(value = "/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get uploads by sourceId")
    public List<UploadDetails> getAllBySourceId(@PathVariable String sourceId, @RequestParam(required = false) Upload.Status status) {
        return uploadService.getAllBySourceId(sourceId, status);
    }

    @GetMapping(value = "/uploadId/{uploadId}")
    @ResponseBody
    @ApiOperation("Get upload by uploadID")
    public UploadDetails getUpload(@PathVariable String uploadId) {
        if (uploadId == null) {
            return null;
        } else {
            return uploadService.getByUploadId(uploadId);
        }
    }

    @GetMapping(value = "/uploadId/{uploadId}/token")
    @ResponseBody
    @ApiOperation("Generate a token for downloading zip file of the upload results")
    public String getToken(@PathVariable String uploadId) {
        return uploadService.generateToken(uploadId);
    }

    @PostMapping("/startimport")
    @ResponseBody
    @ApiOperation(value = "Invoke DCP import workflow. Returns the upload details.")
    public UploadDetails startImport(@RequestBody DCPImportRequest importRequest) {
        return uploadService.startImport(importRequest);
    }
}

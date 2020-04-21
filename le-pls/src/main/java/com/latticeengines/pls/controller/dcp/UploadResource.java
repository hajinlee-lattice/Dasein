package com.latticeengines.pls.controller.dcp;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.pls.service.dcp.UploadService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Uploads")
@RestController
@RequestMapping("/uploads")
@PreAuthorize("hasRole('View_DCP_Projects')")
public class UploadResource {

    private static final Logger log = LoggerFactory.getLogger(UploadResource.class);

    @Inject
    private UploadService uploadService;

    @GetMapping(value = "/sourceId/{sourceId}")
    @ResponseBody
    @ApiOperation("Get sources by sourceId")
    public List<Upload> getAllBySourceId(@PathVariable String sourceId, @RequestParam(required = false) Upload.Status status) {
        return uploadService.getAllBySourceId(sourceId, status);
    }

    @GetMapping(value = "/uploadId/{uploadId}")
    @ResponseBody
    @ApiOperation("Get sources by sourceId")
    public Upload getUpload(@PathVariable Long uploadId) {
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

    @PutMapping(value = "/email/completed")
    @ResponseBody
    @ApiOperation(value = "Send out email after upload completed")
    public void sendUploadCompletedEmail(@RequestBody UploadEmailInfo uploadEmailInfo) {
        uploadService.sendUploadCompleteEmail(uploadEmailInfo);
    }
}

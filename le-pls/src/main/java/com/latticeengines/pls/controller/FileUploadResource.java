package com.latticeengines.pls.controller;

import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.pls.service.FileUploadService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "fileuploads", description = "REST resource for uploading data files")
@RestController
@RequestMapping("/fileuploads")
@PreAuthorize("hasRole('Create_PLS_Models')")
public class FileUploadResource {

    @Autowired
    private FileUploadService fileUploadService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/octet-stream")
    @ResponseBody
    @ApiOperation(value = "Upload a file")
    public SimpleBooleanResponse uploadFile(@PathVariable String fileName, InputStream fileInputStream) {
        fileUploadService.uploadFile(fileName, fileInputStream);
        return SimpleBooleanResponse.successResponse();
    }

}

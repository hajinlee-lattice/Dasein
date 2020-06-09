package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.exception.LedpException;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "s3Import", description = "REST resource for S3 import")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/s3import")
public class S3ImportResource {

    @Inject
    private S3ImportFolderService s3ImportFolderService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;


    @PostMapping("/succeed")
    @ResponseBody
    @ApiOperation(value = "Move file to Succeed folder")
    public ResponseDocument<String> moveToSucceed(@PathVariable String customerSpace, @RequestBody String key) {
        return ResponseDocument.successResponse(s3ImportFolderService.moveFromInProgressToSucceed(key));
    }

    @PostMapping("/failed")
    @ResponseBody
    @ApiOperation(value = "Move file to Succeed folder")
    public ResponseDocument<String> moveToFailed(@PathVariable String customerSpace, @RequestBody String key) {
        return ResponseDocument.successResponse(s3ImportFolderService.moveFromInProgressToFailed(key));
    }

    @PostMapping("/system")
    @ResponseBody
    @ApiOperation(value = "Create an Import System")
    public ResponseDocument<Boolean> createS3ImoprtSystem(@PathVariable String customerSpace, @RequestBody S3ImportSystem system) {
        try {
            s3ImportSystemService.createS3ImportSystem(customerSpace, system);
            return ResponseDocument.successResponse(Boolean.TRUE);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping("/system/update")
    @ResponseBody
    @ApiOperation(value = "Update an Import System")
    public ResponseDocument<Boolean> updateS3ImoprtSystem(@PathVariable String customerSpace,
                                                          @RequestBody S3ImportSystem system) {
        try {
            s3ImportSystemService.updateS3ImportSystem(customerSpace, system);
            return ResponseDocument.successResponse(Boolean.TRUE);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @GetMapping("/system")
    @ResponseBody
    @ApiOperation(value = "Get Import System by system name")
    public S3ImportSystem getS3ImportSystem(@PathVariable String customerSpace, @RequestParam String systemName) {
        return s3ImportSystemService.getS3ImportSystem(customerSpace, systemName);
    }

    @GetMapping("/system/list")
    @ResponseBody
    @ApiOperation(value = "Get All Import Systems")
    public List<S3ImportSystem> getS3ImportSystem(@PathVariable String customerSpace) {
        return s3ImportSystemService.getAllS3ImportSystem(customerSpace);
    }

    @PostMapping("/system/list")
    @ResponseBody
    @ApiOperation(value = "Update All System priority")
    public ResponseDocument<Boolean> updateAllSystemPriority(@PathVariable String customerSpace,
                                                             @RequestBody List<S3ImportSystem> systemList) {
        try {
            s3ImportSystemService.updateAllS3ImportSystemPriority(customerSpace, systemList);
            return ResponseDocument.successResponse(Boolean.TRUE);
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @GetMapping("/system/idList")
    @ResponseBody
    @ApiOperation(value = "Get All system ids")
    public List<String> getS3ImportSystemIdList(@PathVariable String customerSpace) {
        return s3ImportSystemService.getAllS3ImportSystemIds(customerSpace);
    }
}

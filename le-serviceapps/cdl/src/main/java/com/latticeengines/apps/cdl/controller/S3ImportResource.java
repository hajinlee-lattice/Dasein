package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "s3Import", description = "REST resource for S3 import")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/s3import")
public class S3ImportResource {

    @Inject
    private S3ImportFolderService s3ImportFolderService;

    @Inject
    private S3ImportSystemService s3ImportSystemService;


    @RequestMapping(value = "/succeed", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Move file to Succeed folder")
    public ResponseDocument<String> moveToSucceed(@PathVariable String customerSpace, @RequestBody String key) {
        return ResponseDocument.successResponse(s3ImportFolderService.moveFromInProgressToSucceed(key));
    }

    @RequestMapping(value = "/failed", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Move file to Succeed folder")
    public ResponseDocument<String> moveToFailed(@PathVariable String customerSpace, @RequestBody String key) {
        return ResponseDocument.successResponse(s3ImportFolderService.moveFromInProgressToFailed(key));
    }

    @RequestMapping(value = "/system", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create an Import System")
    public void createS3ImoprtSystem(@PathVariable String customerSpace, @RequestBody S3ImportSystem system) {
        s3ImportSystemService.createS3ImportSystem(customerSpace, system);
    }

    @RequestMapping(value = "/system/update", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update an Import System")
    public void updateS3ImoprtSystem(@PathVariable String customerSpace, @RequestBody S3ImportSystem system) {
        s3ImportSystemService.updateS3ImportSystem(customerSpace, system);
    }

    @RequestMapping(value = "/system", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Import System by system name")
    public S3ImportSystem getS3ImoprtSystem(@PathVariable String customerSpace, @RequestParam String systemName) {
        return s3ImportSystemService.getS3ImportSystem(customerSpace, systemName);
    }

    @RequestMapping(value = "/system/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get All Import Systems")
    public List<S3ImportSystem> getS3ImoprtSystem(@PathVariable String customerSpace) {
        return s3ImportSystemService.getAllS3ImportSystem(customerSpace);
    }
}

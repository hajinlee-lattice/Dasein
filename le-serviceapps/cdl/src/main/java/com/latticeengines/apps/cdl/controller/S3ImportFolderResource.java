package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.domain.exposed.ResponseDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data feed task")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/s3import")
public class S3ImportFolderResource {

    @Inject
    private S3ImportFolderService s3ImportFolderService;


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
}

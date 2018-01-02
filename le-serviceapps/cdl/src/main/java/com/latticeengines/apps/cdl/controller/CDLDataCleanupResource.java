package com.latticeengines.apps.cdl.controller;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import jnr.ffi.annotations.In;

@Api(value = "datacleanup", description = "REST resource for cleanup CDL data")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacleanup")
public class CDLDataCleanupResource {

    private final CDLDataCleanupService cdlDataCleanupService;

    @Inject
    public CDLDataCleanupResource(CDLDataCleanupService cdlDataCleanupService) {
        this.cdlDataCleanupService = cdlDataCleanupService;
    }

    @RequestMapping(value = "/all", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clean up all")
    public ResponseDocument<String> cleanupAll(@PathVariable String customerSpace,
                           @RequestParam(value = "BusinessEntity", required = false) BusinessEntity businessEntity) {
        return ResponseDocument.successResponse(
                cdlDataCleanupService.cleanupAll(customerSpace, businessEntity).toString());
    }

    @RequestMapping(value = "/alldata", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clean up all data")
    public ResponseDocument<String> cleanupAllData(@PathVariable String customerSpace,
                           @RequestParam(value = "BusinessEntity", required = false) BusinessEntity businessEntity) {
        return ResponseDocument.successResponse(
                cdlDataCleanupService.cleanupAllData(customerSpace, businessEntity).toString());
    }

    @RequestMapping(value = "/bytimerange", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clean up by time range")
    public ResponseDocument<String> cleanupAll(@PathVariable String customerSpace,
                           @RequestParam(value = "BusinessEntity", required = false) BusinessEntity businessEntity,
                           @RequestParam(value = "startTime") String startTime,
                           @RequestParam(value = "endTime") String endTime) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date start = dateFormat.parse(startTime);
            Date end = dateFormat.parse(endTime);

            return ResponseDocument.successResponse(
                    cdlDataCleanupService.cleanupByTimeRange(customerSpace, businessEntity, start, end).toString());
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }
}

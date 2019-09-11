package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CleanupOperationConfiguration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacleanup", description = "REST resource for cleanup CDL data")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datacleanup")
public class CDLDataCleanupResource {
    private static final Logger log = LoggerFactory.getLogger(CDLDataCleanupResource.class);
    private final CDLDataCleanupService cdlDataCleanupService;

    @Inject
    public CDLDataCleanupResource(CDLDataCleanupService cdlDataCleanupService) {
        this.cdlDataCleanupService = cdlDataCleanupService;
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Clean up data")
    public ResponseDocument<String> cleanup(@PathVariable String customerSpace,
            @RequestBody CleanupOperationConfiguration cleanupOperationConfiguration) {
        try {
            return ResponseDocument.successResponse(
                    cdlDataCleanupService.cleanupData(customerSpace, cleanupOperationConfiguration).toString());
        } catch (Exception e) {
            log.error("error:", e);
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/createReplaceAction", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "create clean up data action")
    public void createReplaceAction(@PathVariable String customerSpace,
                                            @RequestBody CleanupOperationConfiguration cleanupOperationConfiguration) {
        try {
            cdlDataCleanupService.createReplaceAction(customerSpace, cleanupOperationConfiguration);
        } catch (Exception e) {
            log.error("Cannot create cleanup action: {}", e.getMessage());
            throw e;
        }
    }

}

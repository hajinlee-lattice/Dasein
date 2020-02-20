package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.CDLDataCleanupService;
import com.latticeengines.apps.cdl.service.TenantCleanupService;
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
    private final TenantCleanupService tenantCleanupService;

    @Inject
    public CDLDataCleanupResource(CDLDataCleanupService cdlDataCleanupService,
                                  TenantCleanupService tenantCleanupService) {
        this.cdlDataCleanupService = cdlDataCleanupService;
        this.tenantCleanupService = tenantCleanupService;
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

    @PostMapping(value = "/registerDeleteData")
    @ResponseBody
    @ApiOperation(value = "Register delete data table")
    public ResponseDocument<String> registerDeleteData(@PathVariable String customerSpace,
                                                       @RequestParam(value = "user") String user,
                                                       @RequestParam(value = "filename") String filename,
                                                       @RequestParam(value = "hardDelete", required = false,
                                                               defaultValue = "false") boolean hardDelete) {
        try {
            return ResponseDocument.successResponse(cdlDataCleanupService.registerDeleteData(customerSpace,
                    hardDelete, filename, user).toString());
        } catch (RuntimeException e) {
            log.error("Register delete data failed: {}", e.getMessage());
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/replaceAction", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "create clean up data action")
    public void createReplaceAction(@PathVariable String customerSpace,
                                            @RequestBody CleanupOperationConfiguration cleanupOperationConfiguration) {
        try {
            cdlDataCleanupService.createReplaceAction(customerSpace, cleanupOperationConfiguration);
        } catch (Exception e) {
            log.error("Cannot create cleanup action: ", e);
            throw e;
        }
    }

    @RequestMapping(value = "/tenantcleanup", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "clean metadata and redshift of tenant")
    public void tenantCleanup(@PathVariable String customerSpace) {
        // M35
        throw new UnsupportedOperationException("This API reaches the end of life.");
//        try {
//            tenantCleanupService.removeTenantTables(customerSpace);
//        } catch (Exception e) {
//            log.error("error:", e);
//            throw e;
//        }
    }

}

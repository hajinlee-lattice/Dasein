package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ImportMigrateTrackingService;
import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "migratetracking", description = "REST resource for MigrateTracking info")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/migratetracking")
public class MigrateTrackingResource {

    @Inject
    private ImportMigrateTrackingService importMigrateTrackingService;

    @RequestMapping(value = "/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a MigrateTracking record")
    public ImportMigrateTracking createMigrateTracking(@PathVariable String customerSpace) {
        return importMigrateTrackingService.create(customerSpace);
    }

    @RequestMapping(value = "/get/{pid}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a MigrateTracking record by pid")
    public ImportMigrateTracking getMigrateTracking(@PathVariable String customerSpace, @PathVariable Long pid) {
        return importMigrateTrackingService.getByPid(customerSpace, pid);
    }

    @RequestMapping(value = "/actions/{pid}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all registered action list in MigrateTracking report by pid")
    public List<Long> getRegisteredActions(@PathVariable String customerSpace, @PathVariable Long pid) {
        return importMigrateTrackingService.getAllRegisteredActionIds(customerSpace, pid);
    }

    @RequestMapping(value = "/update/{pid}/status/{status}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update MigrateTracking record status by pid")
    public void updateStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                             @PathVariable ImportMigrateTracking.Status status) {
        importMigrateTrackingService.updateStatus(customerSpace, pid, status);
    }

    @RequestMapping(value = "/update/{pid}/report", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update MigrateTracking record report by pid")
    public void updateReport(@PathVariable String customerSpace, @PathVariable Long pid,
                             @RequestBody ImportMigrateReport report) {
        importMigrateTrackingService.updateReport(customerSpace, pid, report);
    }
}

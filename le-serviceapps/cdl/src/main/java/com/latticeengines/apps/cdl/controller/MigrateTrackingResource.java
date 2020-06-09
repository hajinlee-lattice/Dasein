package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ImportMigrateTrackingService;
import com.latticeengines.domain.exposed.cdl.ImportMigrateReport;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "migratetracking", description = "REST resource for MigrateTracking info")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/migratetracking")
public class MigrateTrackingResource {

    @Inject
    private ImportMigrateTrackingService importMigrateTrackingService;

    @PostMapping("/create")
    @ResponseBody
    @ApiOperation(value = "Create a MigrateTracking record")
    public ImportMigrateTracking createMigrateTracking(@PathVariable String customerSpace) {
        return importMigrateTrackingService.create(customerSpace);
    }

    @GetMapping("/get/{pid}")
    @ResponseBody
    @ApiOperation(value = "Get a MigrateTracking record by pid")
    public ImportMigrateTracking getMigrateTracking(@PathVariable String customerSpace, @PathVariable Long pid) {
        return importMigrateTrackingService.getByPid(customerSpace, pid);
    }

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get all MigrateTracking records")
    public List<ImportMigrateTracking> getMigrateTracking(@PathVariable String customerSpace) {
        return importMigrateTrackingService.getAll(customerSpace);
    }

    @GetMapping("/actions/{pid}")
    @ResponseBody
    @ApiOperation(value = "Get all registered action list in MigrateTracking report by pid")
    public List<Long> getRegisteredActions(@PathVariable String customerSpace, @PathVariable Long pid) {
        return importMigrateTrackingService.getAllRegisteredActionIds(customerSpace, pid);
    }

    @PutMapping("/update/{pid}/status/{status}")
    @ResponseBody
    @ApiOperation(value = "Update MigrateTracking record status by pid")
    public void updateStatus(@PathVariable String customerSpace, @PathVariable Long pid,
                             @PathVariable ImportMigrateTracking.Status status) {
        importMigrateTrackingService.updateStatus(customerSpace, pid, status);
    }

    @PutMapping("/update/{pid}/report")
    @ResponseBody
    @ApiOperation(value = "Update MigrateTracking record report by pid")
    public void updateReport(@PathVariable String customerSpace, @PathVariable Long pid,
                             @RequestBody ImportMigrateReport report) {
        importMigrateTrackingService.updateReport(customerSpace, pid, report);
    }
}

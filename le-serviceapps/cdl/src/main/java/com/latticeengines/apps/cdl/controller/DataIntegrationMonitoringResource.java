package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dataintegration", description = "REST resource for data integration monitoring")
@RestController
@RequestMapping("/dataintegration")
public class DataIntegrationMonitoringResource {

    @Inject
    private DataIntegrationStatusMonitoringService dataIntegrationMonitoringService;

    @PostMapping("")
    @ApiOperation(value = "Create data integration status.")
    @NoCustomerSpace
    public boolean createOrUpdateDataIntegrationMonitoring(
            @RequestBody DataIntegrationStatusMonitorMessage status) {
        dataIntegrationMonitoringService.createOrUpdateStatus(status);
        return true;
    }

    @GetMapping("/{eventId}")
    @ApiOperation(value = "Get status of data integration workflow.")
    @NoCustomerSpace
    public DataIntegrationStatusMonitor getDataIntegrationStatus(@PathVariable String eventId) {
        return dataIntegrationMonitoringService.getStatus(eventId);

    }

    @GetMapping("")
    @ApiOperation(value = "Get all workflow statuses for a given tenant.")
    @NoCustomerSpace
    public List<DataIntegrationStatusMonitor> getAllStatuses(
            @RequestParam(value = "tenantId", required = true) String tenantId) {
        return dataIntegrationMonitoringService.getAllStatuses(tenantId);
    }
}

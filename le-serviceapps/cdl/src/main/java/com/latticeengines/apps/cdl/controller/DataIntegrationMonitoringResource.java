package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
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

    /**
     * @param statuses
     * @return Map<String, Boolean> with key representing workflowRequestId and
     *         value the result of the update
     */
    @PostMapping("")
    @ApiOperation(value = "Create data integration status.")
    @NoCustomerSpace
    public Map<String, Boolean> createOrUpdateDataIntegrationMonitoring(
            @RequestBody List<DataIntegrationStatusMonitorMessage> statuses) {
        return dataIntegrationMonitoringService.createOrUpdateStatuses(statuses);
    }

    @GetMapping("/{workflowRequestId}")
    @ApiOperation(value = "Get status of data integration workflow.")
    @NoCustomerSpace
    public DataIntegrationStatusMonitor getDataIntegrationStatus(@PathVariable String workflowRequestId) {
        return dataIntegrationMonitoringService.getStatus(workflowRequestId);

    }

    @GetMapping("{workflowRequestId}/messages")
    @ApiOperation(value = "Get status of data integration workflow.")
    @NoCustomerSpace
    public List<DataIntegrationStatusMessage> getDataIntegrationStatusMessages(@PathVariable String workflowRequestId) {
        return dataIntegrationMonitoringService.getAllStatusMessages(workflowRequestId);

    }

    @GetMapping("")
    @ApiOperation(value = "Get all workflow statuses for a given tenant.")
    @NoCustomerSpace
    public List<DataIntegrationStatusMonitor> getAllStatuses(
            @RequestParam(value = "tenantId", required = true) String tenantId) {
        return dataIntegrationMonitoringService.getAllStatuses(tenantId);
    }
}

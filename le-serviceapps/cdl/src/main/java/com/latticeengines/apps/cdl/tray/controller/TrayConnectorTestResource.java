package com.latticeengines.apps.cdl.tray.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tray", description = "REST resource for Tray Connectors")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/tray/test")
public class TrayConnectorTestResource {

    @Inject
    private TrayConnectorTestService trayConnectorTestService;

    /**
     * @param externalSystemName
     * @param testScenario
     */
    @PostMapping("trigger/connectors/{externalSystemName}/tests/{testScenario}")
    @ApiOperation(value = "Trigger Tray Connector Test.")
    @NoCustomerSpace
    public void triggerTrayConnectorTest(@PathVariable CDLExternalSystemName externalSystemName, //
            @PathVariable String testScenario) {
        trayConnectorTestService.triggerTrayConnectorTest(externalSystemName, testScenario);
    }

    /**
     * @param statuses
     */
    @PostMapping("verify")
    @ApiOperation(value = "Verify Tray Connector Test.")
    @NoCustomerSpace
    public void verifyTrayConnectorTest(
            @RequestBody List<DataIntegrationStatusMonitorMessage> statuses) {
        trayConnectorTestService.verifyTrayConnectorTest(statuses);
    }
}

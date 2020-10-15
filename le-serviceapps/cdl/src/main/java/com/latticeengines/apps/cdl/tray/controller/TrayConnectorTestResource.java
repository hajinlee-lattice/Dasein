package com.latticeengines.apps.cdl.tray.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "trayTrigger", description = "REST resource for Tray connectors test")
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
    public void triggerTrayConnectorTest(@PathVariable String customerSpace, @PathVariable CDLExternalSystemName externalSystemName, //
            @PathVariable String testScenario) {
        trayConnectorTestService.triggerTrayConnectorTest(customerSpace, externalSystemName, testScenario);
    }
}

package com.latticeengines.apps.cdl.tray.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "tray", description = "REST resource for Tray Connectors")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/tray")
public class TrayConnectorTestResource {

    @Inject
    private TrayConnectorTestService trayConnectorTestService;

    /**
     * @param externalSystemName
     * @param testScenario
     */
    @PostMapping("/connectors/{externalSystemName}/tests/{testScenario}")
    @ApiOperation(value = "Create Tray Connector Test.")
    @NoCustomerSpace
    public void createTrayConnectorTest(@PathVariable CDLExternalSystemName externalSystemName, //
            @PathVariable String testScenario) {
        trayConnectorTestService.createTrayConnectorTest(externalSystemName, testScenario);
    }

}

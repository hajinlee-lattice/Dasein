package com.latticeengines.apps.cdl.tray.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.tray.service.TrayConnectorTestService;
import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "trayVerify", description = "REST resource for Tray connectors test verification")
@RestController
@RequestMapping("/tray/test/verify")
public class TrayConnectorTestVerifyResource {

    @Inject
    private TrayConnectorTestService trayConnectorTestService;

    /**
     * @param statuses
     */
    @PostMapping("")
    @ApiOperation(value = "Verify Tray Connector Test.")
    @NoCustomerSpace
    public void verifyTrayConnectorTest(
            @RequestBody List<DataIntegrationStatusMonitorMessage> statuses) {
        trayConnectorTestService.verifyTrayConnectorTest(statuses);
    }
}

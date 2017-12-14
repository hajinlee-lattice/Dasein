package com.latticeengines.apps.cdl.controller;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.MaintenanceOperationService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationConfiguration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "cdlmaintenance", description = "Controller of cdl maintenance.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/maintenance")
public class CDLMaintenanceController {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @RequestMapping(value = "", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "CDL maintain")
    public ResponseDocument<Boolean> maintain(@PathVariable String customerSpace, @RequestBody
                                              MaintenanceOperationConfiguration configuration) {
        MaintenanceOperationService maintenanceOperationService = MaintenanceOperationService.getMaintenanceService
                (configuration.getClass());
        if (maintenanceOperationService == null) {
            throw new RuntimeException(
                    String.format("Cannot find maintenance service for class: %s", configuration.getClass()));
        }
        maintenanceOperationService.invoke(configuration);
        return ResponseDocument.successResponse(true);
    }
}

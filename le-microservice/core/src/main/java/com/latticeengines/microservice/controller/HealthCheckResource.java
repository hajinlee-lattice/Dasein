package com.latticeengines.microservice.controller;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.microservice.service.StatusService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Api(value = "health", description = "REST resource for checking health of microservice and lattice webapps")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    @Autowired
    private StatusService statusService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "check overall health of microservice")
    public StatusDocument checkHealth() {
        Map<String, String> status = statusService.moduleStatus();
        if (status.containsKey("Overall") && "OK".equals(status.get("Overall"))) {
            return StatusDocument.up();
        } else {
            return StatusDocument.down();
        }
    }

    @RequestMapping(value = "/apps/detail", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "check detail status of lattice webapps")
    public Map<String, String> checkApps() {
        return statusService.appStatus();
    }

    @RequestMapping(value = "/apps", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "check overall health of lattice webapps")
    public StatusDocument checkAppsHealth() {
        Map<String, String> status = statusService.appStatus();
        if (status.containsKey("Overall") && "OK".equals(status.get("Overall"))) {
            return StatusDocument.up();
        } else {
            return StatusDocument.down();
        }
    }

    @RequestMapping(value = "/apps", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "unhookApp an app from health check for now. It will be automatically monitored again, once the health of it becomes OK.")
    public SimpleBooleanResponse unhookApp(@RequestParam(value = "app") String app) {
        try {
            statusService.unhookApp(app);
            return SimpleBooleanResponse.successResponse();
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(e);
        }
    }

}

package com.latticeengines.microservice.controller;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.microservice.service.StatusService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "status", description = "REST resource for checking status of all microservices")
@RestController
@RequestMapping("/status")
public class StatusController {

    @Autowired
    private StatusService statusService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "check status of all the microservices modules")
    public Map<String, String> checkModules() {
        return statusService.moduleStatus();
    }

}

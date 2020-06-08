package com.latticeengines.microservice.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.microservice.service.StatusService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "status", description = "REST resource for checking status of all microservices")
@RestController
@RequestMapping("/status")
public class StatusController {

    @Inject
    private StatusService statusService;

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "check status of all the microservices modules")
    public Map<String, String> checkModules() {
        return statusService.moduleStatus();
    }

}

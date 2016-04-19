package com.latticeengines.pls.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of LP app")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    public static final String MESSAGE = "{\"status\":\"online\"}";

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    public String healthCheck() {
        return MESSAGE;
    }
}
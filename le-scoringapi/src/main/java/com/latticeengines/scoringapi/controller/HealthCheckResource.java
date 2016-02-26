package com.latticeengines.scoringapi.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.DetailedErrors;

@Api(value = "score", description = "REST resource for checking health of score API")
@RestController
@RequestMapping("/health")
@DetailedErrors
public class HealthCheckResource {

    public static final String MESSAGE = "Score API is online.";

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    public String healthCheck() {
        // TODO Add actual health checking; right now it just checks that the
        // WAR has been initialized successfully.

        return MESSAGE;
    }
}

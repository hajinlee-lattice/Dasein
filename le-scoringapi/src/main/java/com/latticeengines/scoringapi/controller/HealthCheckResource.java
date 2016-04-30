package com.latticeengines.scoringapi.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of score API")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    public StatusDocument healthCheck() {
        // TODO Add actual health checking; right now it just checks that the
        // WAR has been initialized successfully.

        return StatusDocument.online();
    }
}

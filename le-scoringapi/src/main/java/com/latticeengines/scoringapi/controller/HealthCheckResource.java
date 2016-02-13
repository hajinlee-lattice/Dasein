package com.latticeengines.scoringapi.controller;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.rest.DetailedErrors;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "score", description = "REST resource for checking health of score API")
@RestController
@RequestMapping("/health")
@DetailedErrors
public class HealthCheckResource {

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ApiOperation(value = "Health check")
    public void healthCheck(HttpServletResponse response) {
        // TODO Add actual health checking; right now it just checks that the
        // WAR has been initialized successfully.

        response.setContentLength(0);
        response.setStatus(HttpServletResponse.SC_OK);
    }
}

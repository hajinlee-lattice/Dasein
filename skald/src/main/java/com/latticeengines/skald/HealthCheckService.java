package com.latticeengines.skald;

import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckService {
    @RequestMapping(value = "HealthCheck", method = RequestMethod.GET)
    public void healthCheck(HttpServletResponse response) {
        // TODO Add actual health checking; right now it just checks that the
        // WAR has been initialized successfully.

        response.setContentLength(0);
        response.setStatus(HttpServletResponse.SC_OK);
    }
}

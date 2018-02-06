package com.latticeengines.oauth2.authserver;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "health", description = "REST resource for checking health")
@RestController
@RequestMapping("/oauth/health")
public class HealthResource {

    @GetMapping("")
    @ResponseBody
    @ApiIgnore
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }
}

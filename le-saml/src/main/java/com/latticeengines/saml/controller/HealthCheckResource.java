package com.latticeengines.saml.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of saml API")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    private static final Logger log = LoggerFactory.getLogger(HealthCheckResource.class);

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

//    @GetMapping("/log-test")
//    @ResponseBody
//    @NoMetricsLog
//    public String testLog(@RequestParam(value = "level", required = false, defaultValue = "INFO") String levelName) throws Exception {
//        Level level = Level.toLevel(levelName);
//        log.info("Attempt to change to " + level);
//        try {
//            if (Level.DEBUG.equals(level)) {
//                ThreadContext.put("UserLevel", "DEBUG");
//            }
//            for (int i = 0; i < 100; i++) {
//                log.info("I am logging at INFO");
//                log.debug("I am logging at DEBUG");
//                Thread.sleep(1000);
//            }
//            return "done";
//        } finally {
//            ThreadContext.clearAll();
//        }
//    }

}

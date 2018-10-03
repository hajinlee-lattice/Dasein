package com.latticeengines.sqoop.controller;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.sqoop.util.YarnConfigurationUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of sqoop API")
@RestController
@RequestMapping("/health")
public class HealthResource {

    private static final Logger log = LoggerFactory.getLogger(HealthResource.class);

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

    @GetMapping(value = "/yarn-config")
    @ResponseBody
    @ApiOperation(value = "Check Yarn Configuration")
    @NoMetricsLog
    public String checkYarnConfiguration() {
        YarnConfiguration yarnConfiguration = YarnConfigurationUtils.getYarnConfiguration();
        String defaultFs = yarnConfiguration.get("fs.defaultFS");
        log.info("Use yarnConfiguration with default Fs: " + defaultFs);
        return defaultFs;
    }

}

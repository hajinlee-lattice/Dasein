package com.latticeengines.matchapi.controller;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import reactor.core.publisher.Mono;
import springfox.documentation.annotations.ApiIgnore;

@SuppressWarnings("deprecation")
@Api(value = "internal", description = "Internal REST resource for match api.")
@RestController
@RequestMapping("/internal")
public class InternalResource extends InternalResourceBase {

    @Inject
    private DataCloudYarnService yarnService;

    @Resource(name = "yarnConfiguration")
    private Configuration yarnConfiguration;

    private Map<String, String> props = null;

    @PostMapping(value = "/yarnjobs")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Match a block of input data in yarn container")
    public AppSubmission submitYarnJob(@RequestBody DataCloudJobConfiguration jobConfiguration) {
        ApplicationId applicationId = yarnService.submitPropDataJob(jobConfiguration);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

    @GetMapping(value = "/yarn-config")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Match a block of input data in yarn container")
    public Map<String, String> getEMRConfiguration() {
        if (props == null) {
            synchronized (this) {
                if (props == null) {
                    props = new HashMap<>();
                    yarnConfiguration.forEach(entry -> props.put(entry.getKey(), entry.getValue()));
                }
            }
        }
        return props;
    }

    // an api to test reactive endpoint
    @GetMapping(value = "/mono")
    @ResponseBody
    public Mono<String> submitYarnJob() {
        return Mono.just("Hello!");
    }

}

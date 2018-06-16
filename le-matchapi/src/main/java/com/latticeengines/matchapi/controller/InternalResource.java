package com.latticeengines.matchapi.controller;

import java.util.Collections;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.security.Tenant;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "internal", description = "Internal REST resource for match api.")
@RestController
@RequestMapping("/internal")
public class InternalResource extends InternalResourceBase {

    @Autowired
    private DataCloudYarnService yarnService;

    @PostMapping(value = "/yarnjobs", produces = "application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Match a block of input data in yarn container")
    public AppSubmission submitYarnJob(@RequestBody DataCloudJobConfiguration jobConfiguration,
            HttpServletRequest request) {
        checkHeader(request);
        ApplicationId applicationId = yarnService.submitPropDataJob(jobConfiguration);
        return new AppSubmission(Collections.singletonList(applicationId));
    }

}

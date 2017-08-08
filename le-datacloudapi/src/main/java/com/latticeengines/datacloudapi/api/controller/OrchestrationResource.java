package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "orchestration", description = "REST resource for orchestrations")
@RestController
@RequestMapping("/orchestrations")
public class OrchestrationResource {

    @Autowired
    private OrchestrationService orchestrationService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all engine jobs that can proceed.")
    public List<OrchestrationProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        return orchestrationService.scan(hdfsPod);
    }
}

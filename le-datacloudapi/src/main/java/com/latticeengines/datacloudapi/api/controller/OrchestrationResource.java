package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "orchestration", description = "REST resource for orchestrations")
@RestController
@RequestMapping("/orchestrations")
public class OrchestrationResource {

    @Inject
    private OrchestrationService orchestrationService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Scan and trigger all engine jobs that can proceed.")
    public List<OrchestrationProgress> scan(
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        return orchestrationService.scan(hdfsPod);
    }

    @GetMapping("progresses/engine/{engine}/name/{engineName}/version/{version}")
    @ResponseBody
    @ApiOperation(value = "Find status of the job in particular engine")
    public DataCloudEngineStage getProgress(
            @PathVariable String engineName, @PathVariable DataCloudEngine engine, @PathVariable String version,
            @RequestParam(value = "HdfsPod", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            DataCloudEngineStage jobConfig = new DataCloudEngineStage(engine, engineName, version);
            DataCloudEngineStage status = orchestrationService.getDataCloudEngineStatus(jobConfig);
            if (status == null) {
                throw new IllegalStateException("Cannot get status of the current job");
            }
            return status;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }
}

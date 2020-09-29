package com.latticeengines.matchapi.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.match.service.VboUsageService;
import com.latticeengines.domain.exposed.datacloud.usage.SubmitBatchReportRequest;
import com.latticeengines.domain.exposed.datacloud.usage.VboBatchUsageReport;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "VBO usage tracking")
@RestController
@RequestMapping("/usage")
public class UsageResource {

    @Inject
    private VboUsageService vboUsageService;

    @PostMapping("/batch")
    @ResponseBody
    @ApiOperation(value = "Get all block metadata")
    public VboBatchUsageReport getBlockMetadata(@RequestBody SubmitBatchReportRequest request) {
        return vboUsageService.submitBatchUsageReport(request);
    }

}

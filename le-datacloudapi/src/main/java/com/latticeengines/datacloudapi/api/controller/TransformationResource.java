package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "transform", description = "REST resource for source transformation")
@RestController
@RequestMapping("/transformations")
public class TransformationResource {

    @Inject
    private SourceTransformationService sourceTransformationService;

    @PostMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Scan all transformation progresses that can be proceeded. "
            + "url parameter podid is for testing purpose.")
    public List<TransformationProgress> scan(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        if (StringUtils.isEmpty(hdfsPod)) {
            hdfsPod = HdfsPodContext.getHdfsPodId();
        }
        return sourceTransformationService.scan(hdfsPod);
    }

    @PostMapping("internal")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Trigger a new transformation for a source at its latest version. "
            + "If a transformation with the same source version already exists, skip operation. "
            + "url parameter submitter indicates what submitted this job: Quartz, Test, Cli, ..."
            + "url parameter podid is for testing purpose.")
    public TransformationProgress transform(@RequestBody TransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            checkTransformationRequest(transformationRequest);
            TransformationProgress progress = sourceTransformationService.transform(transformationRequest, hdfsPod,
                    false);
            if (progress == null) {
                throw new IllegalStateException("Cannot start a new progress for your request");
            }
            return progress;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }

    private void checkTransformationRequest(TransformationRequest transformationRequest) {
        if (transformationRequest.getSourceBeanName().equals("bomboraWeeklyAggService")) {
            if (StringUtils.isEmpty(transformationRequest.getTargetVersion())) {
                throw new IllegalArgumentException("Please provide aggregation date in TargetVersion field");
            }
            if (CollectionUtils.isEmpty(transformationRequest.getBaseVersions())) {
                throw new IllegalArgumentException("Please provide BaseVersion for BomboraDepivoted");
            }
        }
    }

    @PostMapping("pipeline")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Trigger a new transformation for a pipelined transformations. ")
    public TransformationProgress transform(@RequestBody PipelineTransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            TransformationProgress progress = sourceTransformationService.pipelineTransform(transformationRequest,
                    hdfsPod);
            if (progress == null) {
                throw new IllegalStateException("Cannot start a new progress for your request");
            }
            return progress;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
            RequestContext.cleanErrors();
        }
    }

    @PostMapping("pipelineconf")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Get workflow configuration for a pipelined transformations. ")
    public TransformationWorkflowConfiguration getWorkflowConf(
            @RequestBody PipelineTransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            return sourceTransformationService.generatePipelineWorkflowConf(transformationRequest, hdfsPod);
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }

    @GetMapping("progress")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Get the TransformationProgress.")
    public TransformationProgress getProgress(@RequestParam(value = "rootOperationUid") String rootOperationUid,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod) {
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            TransformationProgress progress = sourceTransformationService.getProgress(rootOperationUid);
            if (progress == null) {
                throw new IllegalStateException("Cannot find progress for rootOperationUid=" + rootOperationUid);
            }
            return progress;
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }
}

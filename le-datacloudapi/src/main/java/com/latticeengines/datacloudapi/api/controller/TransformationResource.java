package com.latticeengines.datacloudapi.api.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloudapi.engine.transformation.service.SourceTransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.TransformationRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.network.exposed.propdata.TransformationInterface;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import springfox.documentation.annotations.ApiIgnore;

@Api(value = "transform", description = "REST resource for source transformation")
@RestController
@RequestMapping("/transformations")
public class TransformationResource extends InternalResourceBase implements TransformationInterface {

    @Autowired
    private SourceTransformationService sourceTransformationService;

    @Override
    public List<TransformationProgress> scan(String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @Override
    public TransformationProgress transform(TransformationRequest transformationRequest, String hdfsPod) {
        throw new UnsupportedOperationException("This is a place holder of a proxy method.");
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Scan all transformation progresses that can be proceeded. "
            + "url parameter podid is for testing purpose.")
    public List<TransformationProgress> scan(
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getHdfsPodId();
            }

            return sourceTransformationService.scan(hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25009, e);
        }
    }

    @RequestMapping(value = "internal", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Trigger a new transformation for a source at its latest version. "
            + "If a transformation with the same source version already exists, skip operation. "
            + "url parameter submitter indicates what submitted this job: Quartz, Test, Cli, ..."
            + "url parameter podid is for testing purpose.")
    public TransformationProgress transform(@RequestBody TransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
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
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25011, e, new String[] { transformationRequest.getSourceBeanName() });
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

    @RequestMapping(value = "pipeline", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiIgnore
    @ApiOperation(value = "Trigger a new transformation for a pipelined transformations. ")
    public TransformationProgress transform(@RequestBody PipelineTransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request) {
        checkHeader(request);
        try {
            if (StringUtils.isEmpty(hdfsPod)) {
                hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
                HdfsPodContext.changeHdfsPodId(hdfsPod);
            }
            TransformationProgress progress = sourceTransformationService.pipelineTransform(transformationRequest, hdfsPod);
            if (progress == null) {
                throw new IllegalStateException("Cannot start a new progress for your request");
            }
            return progress;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25011, e, new String[] { "Failed to start pipeline transformation" });
        } finally {
            hdfsPod = HdfsPodContext.getDefaultHdfsPodId();
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
    }
}

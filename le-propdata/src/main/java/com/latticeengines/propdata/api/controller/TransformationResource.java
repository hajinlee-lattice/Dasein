package com.latticeengines.propdata.api.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.transformation.TransformationRequest;
import com.latticeengines.network.exposed.propdata.TransformationInterface;
import com.latticeengines.propdata.engine.transformation.service.SourceTransformationService;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

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
            return sourceTransformationService.scan(hdfsPod);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25009, e);
        }
    }

    @RequestMapping(value = "internal", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Trigger a new transformation for a source at its latest version. "
            + "If a transformation with the same source version already exists, skip operation. "
            + "url parameter submitter indicates what submitted this job: Quartz, Test, Cli, ..."
            + "url parameter podid is for testing purpose.")
    public TransformationProgress transform(@RequestBody TransformationRequest transformationRequest,
            @RequestParam(value = "podid", required = false, defaultValue = "") String hdfsPod,
            HttpServletRequest request, HttpServletResponse response) {
        checkHeader(request);
        try {
            TransformationProgress progress = sourceTransformationService.transform(transformationRequest, hdfsPod);
            if (progress == null) {
                response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                response.getWriter().write("No unprocessed version found.");
                response.getWriter().flush();
                response.getWriter().close();
                return null;
            }
            response.setStatus(HttpServletResponse.SC_ACCEPTED);
            return progress;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25011, e, new String[] { transformationRequest.getSourceBeanName() });
        }
    }
}

package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.service.SegmentService;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "segment", description = "REST resource for segments")
@RestController
@RequestMapping("/segments")
@PreAuthorize("hasRole('Edit_PLS_Models')")
public class SegmentResource {

    private static final Log log = LogFactory.getLog(SegmentResource.class);

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private SegmentService segmentService;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private TenantConfigServiceImpl tenantConfigService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Deprecated
    @RequestMapping(value = "/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public Segment getSegmentByName(@PathVariable String segmentName) {
        return segmentEntityMgr.findByName(segmentName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of segments")
    public List<Segment> getSegments(@RequestParam(value = "selection", required = false) String selection,
            HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        List<Segment> segments = dataLoaderService.getSegments(CustomerSpace.parse(tenant.getId()).getTenantId(),
                tenantConfigService.getDLRestServiceAddress(tenant.getId()));
        log.info("getSegments:" + segments.toString());

        return segments;
    }

    @Deprecated
    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a segment")
    public ResponseDocument<?> createSegment(@RequestBody Segment segment, HttpServletRequest request) {
        try {
            segmentService.createSegment(segment, request);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @Deprecated
    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a segment")
    public ResponseDocument<?> delete(@PathVariable String segmentName) {
        Segment segment = segmentEntityMgr.findByName(segmentName);
        try {
            segmentEntityMgr.delete(segment);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @Deprecated
    @RequestMapping(value = "/{segmentName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a segment")
    public ResponseDocument<?> update(@PathVariable String segmentName, @RequestBody Segment newSegment) {
        try {
            segmentService.update(segmentName, newSegment);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/list", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Updates list of segments")
    public ResponseDocument<?> updateSegments(@RequestBody List<Segment> segments, HttpServletRequest request) {
        try {
            Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
            log.info("updateSegments:" + segments);
            dataLoaderService.setSegments(CustomerSpace.parse(tenant.getId()).getTenantId(),
                    tenantConfigService.getDLRestServiceAddress(tenant.getId()), segments);

            // Models assigned to segments are considered active, those
            // otherwise are inactive.
            Set<String> modelIds = new HashSet<>();
            for (Segment segment : segments) {
                modelIds.add(segment.getModelId());
            }
            for (ModelSummary model : modelSummaryEntityMgr.findAllValid()) {
                ModelSummaryStatus modelStatus = ModelSummaryStatus.INACTIVE;
                if (modelIds.contains(model.getId())) {
                    modelStatus = ModelSummaryStatus.ACTIVE;
                }
                modelSummaryEntityMgr.updateStatusByModelId(model.getId(), modelStatus);
            }

            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils
                    .getFullStackTrace(e)));
        }
    }

}

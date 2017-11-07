package com.latticeengines.pls.controller.datacollection;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "data-collection-segments", description = "REST resource for segments")
@RestController
@RequestMapping("/datacollection/segments")
public class MetadataSegmentResource {

    private final MetadataSegmentService metadataSegmentService;
    private final SessionService sessionService;

    @Inject
    public MetadataSegmentResource(MetadataSegmentService metadataSegmentService, SessionService sessionService) {
        this.metadataSegmentService = metadataSegmentService;
        this.sessionService = sessionService;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments() {
        return metadataSegmentService.getSegments();
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment with name")
    public MetadataSegment getSegmentByName(@PathVariable String segmentName) {
        return metadataSegmentService.getSegmentByName(segmentName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update a segment by name")
    public MetadataSegment createOrUpdateSegmentWithName(@RequestBody MetadataSegment metadataSegment) {
        Object principal = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        if (principal != null) {
            String email = principal.toString();
            if (StringUtils.isNotBlank(email)) {
                metadataSegment.setCreatedBy(email);
            }
        }
        return metadataSegmentService.createOrUpdateSegment(metadataSegment);
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a segment by name")
    public void deleteSegmentByName(@PathVariable String segmentName) {
        metadataSegmentService.deleteSegmentByName(segmentName);
    }

    @RequestMapping(value = "/export", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update a segment export job")
    public MetadataSegmentExport createOrUpdateSegmentExportJob(
            @RequestBody MetadataSegmentExport metadataSegmentExportJob, HttpServletRequest request) {
        Session session = SecurityUtils.getSessionFromRequest(request, sessionService);
        if (session != null) {
            String email = session.getEmailAddress();
            if (StringUtils.isNotBlank(email)) {
                metadataSegmentExportJob.setCreatedBy(email);
            }
        }
        return metadataSegmentExportJob;
    }

    @RequestMapping(value = "/export", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of segment export job")
    public List<MetadataSegmentExport> getSegmentExportJobs(HttpServletRequest request) {
        return new ArrayList<>();
    }

    @RequestMapping(value = "/export/{name}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment export job")
    public MetadataSegmentExport getSegmentExportJob(@PathVariable String name, HttpServletRequest request) {
        return new MetadataSegmentExport();
    }

    @RequestMapping(value = "/export/cleanup", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cleanup expired export jobs")
    public void cleanupExpiredJobs(@PathVariable String name, HttpServletRequest request) {
    }
}

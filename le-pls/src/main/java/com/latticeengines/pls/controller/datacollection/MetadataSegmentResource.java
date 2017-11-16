package com.latticeengines.pls.controller.datacollection;

import java.util.List;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.mortbay.log.Log;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.pls.service.MetadataSegmentExportService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.security.exposed.service.SessionService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "data-collection-segments", description = "REST resource for segments")
@RestController
@RequestMapping("/datacollection/segments")
public class MetadataSegmentResource {

    private final MetadataSegmentService metadataSegmentService;
    private final MetadataSegmentExportService metadataSegmentExportService;
    private final SessionService sessionService;

    @Inject
    public MetadataSegmentResource(MetadataSegmentService metadataSegmentService,
            MetadataSegmentExportService metadataSegmentExportService, SessionService sessionService) {
        this.metadataSegmentService = metadataSegmentService;
        this.metadataSegmentExportService = metadataSegmentExportService;
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
    @ApiOperation(value = "Create a segment export job")
    public MetadataSegmentExport createSegmentExportJob(@RequestBody MetadataSegmentExport metadataSegmentExportJob) {
        return metadataSegmentExportService.createSegmentExportJob(metadataSegmentExportJob);
    }

    @RequestMapping(value = "/export", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of segment export job")
    public List<MetadataSegmentExport> getSegmentExportJobs(HttpServletRequest request) {
        return metadataSegmentExportService.getSegmentExports();
    }

    @RequestMapping(value = "/export/{exportId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment export job")
    public MetadataSegmentExport getSegmentExportJob(@PathVariable String exportId) {
        return metadataSegmentExportService.getSegmentExportByExportId(exportId);
    }

    @RequestMapping(value = "/export/{exportId}/download", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Download result of export job")
    public void downloadSegmentExportResult(@PathVariable String exportId, HttpServletRequest request,
            HttpServletResponse response) {
        Log.info("Received call for downloading result of job " + exportId);
        metadataSegmentExportService.downloadSegmentExportResult(exportId, request, response);
    }
}

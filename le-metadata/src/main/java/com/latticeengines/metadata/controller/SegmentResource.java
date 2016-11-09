package com.latticeengines.metadata.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.service.SegmentService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata segments")
@RestController
@RequestMapping("/customerspaces/{customerSpace}")
public class SegmentResource {
    
    @Autowired
    private SegmentService segmentService;

    @RequestMapping(value = "/segments", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace, HttpServletRequest request) {
        return segmentService.getSegments();
    }

    @RequestMapping(value = "/segments/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public MetadataSegment getSegment(@PathVariable String customerSpace, //
            @PathVariable String segmentName, //
            HttpServletRequest request) {
        return segmentService.findByName(segmentName);
    }

    @RequestMapping(value = "/segments/{segmentName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create segment")
    public MetadataSegment createSegment(@PathVariable String customerSpace, //
            @PathVariable String segmentName, //
            @RequestParam(value = "tableName", required = true) String tableName, //
            HttpServletRequest request) {
        return segmentService.createSegment(customerSpace, segmentName, tableName);
    }

}

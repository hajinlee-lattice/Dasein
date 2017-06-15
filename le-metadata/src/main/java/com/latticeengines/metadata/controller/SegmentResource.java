package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.service.SegmentService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata segments")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/segments")
public class SegmentResource {

    @Autowired
    private SegmentService segmentService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        return segmentService.getSegments(customerSpace);
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public MetadataSegment getSegment(@PathVariable String customerSpace, //
            @PathVariable String segmentName) {
        return segmentService.findByName(customerSpace, segmentName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a segment")
    public MetadataSegment createOrUpdateSegment(@PathVariable String customerSpace, //
            @RequestBody MetadataSegment segment) {
        return segmentService.createOrUpdateSegment(customerSpace, segment);
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a segment by name")
    public Boolean deleteSegmentByName(@PathVariable String customerSpace, @PathVariable String segmentName) {
        return segmentService.deleteSegmentByName(customerSpace, segmentName);
    }

}

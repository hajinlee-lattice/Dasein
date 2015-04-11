package com.latticeengines.pls.controller;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ResponseDocument;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.pls.SimpleBooleanResponse;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.service.SegmentService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "segment", description = "REST resource for segments")
@RestController
@RequestMapping("/segments")
@PreAuthorize("hasRole('Edit_PLS_Models')")
public class SegmentResource {
    
    @Autowired
    private SegmentEntityMgr segmentEntityMgr;
    
    @Autowired
    private SegmentService segmentService;
    
    @RequestMapping(value = "/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment by name")
    public Segment getSegmentByName(@PathVariable String segmentName) {
        return segmentEntityMgr.findByName(segmentName);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of segments")
    public List<Segment> getSegments(@RequestParam(value="selection", required=false) String selection) {
        return segmentEntityMgr.findAll();
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a segment")
    public ResponseDocument<?> createSegment(@RequestBody Segment segment, HttpServletRequest request) {
        try {
            segmentService.createSegment(segment, request);
            return SimpleBooleanResponse.getSuccessResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { e.getMessage() }));
        } catch (Exception e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { ExceptionUtils.getFullStackTrace(e) }));
        }
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a segment")
    public ResponseDocument<?> delete(@PathVariable String segmentName) {
        Segment segment = segmentEntityMgr.findByName(segmentName);
        try {
            segmentEntityMgr.delete(segment);
            return SimpleBooleanResponse.getSuccessResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { e.getMessage() }));
        } catch (Exception e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { ExceptionUtils.getFullStackTrace(e) }));
        }
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a segment")
    public ResponseDocument<?> update(@PathVariable String segmentName, @RequestBody Segment newSegment) {
        try {
            segmentService.update(segmentName, newSegment);
            return SimpleBooleanResponse.getSuccessResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { e.getMessage() }));
        } catch (Exception e) {
            return SimpleBooleanResponse.getFailResponse(Arrays.<String>asList(new String[] { ExceptionUtils.getFullStackTrace(e) }));
        }
    }

}

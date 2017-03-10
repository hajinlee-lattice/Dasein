package com.latticeengines.pls.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;
import com.wordnik.swagger.annotations.ApiOperation;
import io.swagger.annotations.Api;

@Api(value = "metadatasegments", description = "REST resource for metadata segments")
@RestController
@RequestMapping("/metadatasegments")
public class MetadataSegmentResource {

    @Autowired
    private SessionService sessionService;

    @Autowired
    private MetadataProxy metadataProxy;

    @RequestMapping(value = "/all", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all segments")
    public List<MetadataSegment> getSegments(HttpServletRequest httpRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(httpRequest, sessionService);
        return metadataProxy.getMetadataSegments(CustomerSpace.parse(tenant.getId()).toString()
        );
    }

    @RequestMapping(value = "/name/{segmentName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get segment with name")
    public MetadataSegment getSegmentByName(@PathVariable String segmentName, //
            HttpServletRequest httpServletRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(httpServletRequest, sessionService);
        return metadataProxy.getMetadataSegmentByName(
                CustomerSpace.parse(tenant.getId()).toString(), segmentName);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create or update a segment by name")
    public MetadataSegment createOrUpdateSegmentWithName(
            @RequestBody MetadataSegment metadataSegment, HttpServletRequest httpServletRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(httpServletRequest, sessionService);
        return metadataProxy.createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(),
                metadataSegment);
    }

    @RequestMapping(value = "/{segmentName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ApiOperation(value = "Delete a segment by name")
    public void deleteSegmentByName(@PathVariable String segmentName,
            HttpServletRequest httpServletRequest) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(httpServletRequest, sessionService);
        metadataProxy.deleteSegmentByName(CustomerSpace.parse(tenant.getId()).toString(),
                segmentName);
    }

}

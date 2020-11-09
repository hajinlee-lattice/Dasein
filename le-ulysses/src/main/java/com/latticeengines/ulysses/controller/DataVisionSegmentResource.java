package com.latticeengines.ulysses.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.ListSegment;
import com.latticeengines.domain.exposed.metadata.ListSegmentRequest;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;

import io.swagger.annotations.Api;

@Api(value = "APIs datavision segment")
@RestController
@RequestMapping("/datavision/{customerSpace}/segments")
public class DataVisionSegmentResource {

    @Inject
    private SegmentProxy segmentProxy;

    @GetMapping("")
    public List<MetadataSegment> getSegments(@PathVariable String customerSpace) {
        return segmentProxy.getLiSegments(customerSpace);
    }

    @GetMapping("/{externalSystem}/{externalSegmentId}")
    public MetadataSegment getSegmentByExternalInfo(@PathVariable String customerSpace, @PathVariable String externalSystem,
                                       @PathVariable String externalSegmentId) {
        return segmentProxy.getListSegmentByExternalInfo(customerSpace, externalSystem, externalSegmentId);
    }

    @PostMapping("")
    public MetadataSegment createOrUpdateSegment(@PathVariable String customerSpace, @RequestBody ListSegmentRequest request) {
        return segmentProxy.createOrUpdateListSegment(customerSpace, convertFromRequest(request));
    }

    private MetadataSegment convertFromRequest(ListSegmentRequest request) {
        MetadataSegment metadataSegment = new MetadataSegment();
        metadataSegment.setDisplayName(request.getDisplayName());
        ListSegment listSegment = new ListSegment();
        listSegment.setExternalSystem(request.getExternalSystem());
        listSegment.setExternalSegmentId(request.getExternalSegmentId());
        metadataSegment.setListSegment(listSegment);
        return metadataSegment;
    }

    @DeleteMapping("")
    public boolean deleteSegment(@PathVariable String customerSpace, @RequestBody ListSegmentRequest request) {
        segmentProxy.deleteSegmentByExternalInfo(customerSpace, convertFromRequest(request), false);
        return true;
    }
}

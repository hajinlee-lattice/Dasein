package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
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
@RequestMapping("/datavision/segments")
public class DataVisionSegmentResource {

    @Inject
    private SegmentProxy segmentProxy;

    @GetMapping
    public String getSegments() {
        return "Hello world!";
    }

    @PostMapping
    public MetadataSegment createOrUpdateSegment(@RequestBody ListSegmentRequest request) {
        return segmentProxy.createOrUpdateListSegment(request.getTenantId(), convertFromRequest(request));
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

    @DeleteMapping
    public MetadataSegment deleteSegment(@RequestBody ListSegmentRequest request) {
        return null;
    }
}

package com.latticeengines.ulysses.controller;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
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
    public List<ListSegmentSummary> getSegments(@PathVariable String customerSpace) {
        List<MetadataSegment> segmentList = segmentProxy.getLiSegments(customerSpace);
        if (CollectionUtils.isNotEmpty(segmentList)) {
            return segmentList.stream().map(ListSegmentSummary::fromMetadataSegment).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @GetMapping("/{externalSystem}/{externalSegmentId}")
    public ListSegmentSummary getSegmentByExternalInfo(@PathVariable String customerSpace, @PathVariable String externalSystem,
                                                    @PathVariable String externalSegmentId) {
        MetadataSegment segment = segmentProxy.getListSegmentByExternalInfo(customerSpace, externalSystem, externalSegmentId);
        return ListSegmentSummary.fromMetadataSegment(segment);
    }

    @PostMapping("")
    public ListSegmentSummary createOrUpdateSegment(@PathVariable String customerSpace, @RequestBody ListSegmentRequest request) {
        MetadataSegment segment =  segmentProxy.createOrUpdateListSegment(customerSpace, convertFromRequest(request));
        return ListSegmentSummary.fromMetadataSegment(segment);
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

    @DeleteMapping("/{externalSystem}/{externalSegmentId}")
    public boolean deleteSegment(@PathVariable String customerSpace, @PathVariable String externalSystem, @PathVariable String externalSegmentId) {
        segmentProxy.deleteSegmentByExternalInfo(customerSpace, externalSystem, externalSegmentId, false);
        return true;
    }

}

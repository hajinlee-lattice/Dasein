package com.latticeengines.testframework.exposed.proxy.pls;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

@Component("testMetadataSegmentProxy")
public class TestMetadataSegmentProxy extends PlsRestApiProxyBase {

    public TestMetadataSegmentProxy() {
        super("pls/datacollection/segments");
    }

    public List<MetadataSegment> getSegments() {
        List<?> raw = get("getSegments", constructUrl("/"), List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    public MetadataSegment getSegment(String segmentName) {
        return get("getSegment", constructUrl("/{segmentName}", segmentName), MetadataSegment.class);
    }

    public MetadataSegment createOrUpdate(MetadataSegment segment) {
        return post("createOrUpdateSegment", constructUrl("/"), segment, MetadataSegment.class);
    }

    public void updateCounts(String segmentName) {
        put("updateSegmentCounts", constructUrl("/{segmentName}/counts", segmentName), null);
    }

    public MetadataSegmentExport createSegmentExport(MetadataSegmentExport metadataSegmentExport, Boolean useSpark) {
        return post("createSegmentExport", constructUrl("/export?useSpark={useSpark}", useSpark), metadataSegmentExport,
                MetadataSegmentExport.class);
    }

    public MetadataSegmentExport getSegmentExport(String exportId) {
        return get("getSegmentExport", constructUrl("/export/{exportId}", exportId), MetadataSegmentExport.class);
    }

}

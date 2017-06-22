package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("segmentProxy")
public class SegmentProxy extends MicroserviceRestApiProxy {

    protected SegmentProxy() {
        super("metadata/customerspaces");
    }

    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment metadataSegment) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments", //
                shortenCustomerSpace(customerSpace));
        return post("createOrUpdateSegment", url, metadataSegment, MetadataSegment.class);
    }

    public MetadataSegment getMetadataSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments/{segmentName}", //
                shortenCustomerSpace(customerSpace), segmentName);
        return get("getSegment", url, MetadataSegment.class);
    }

    @SuppressWarnings("rawtypes")
    public List<MetadataSegment> getMetadataSegments(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments", shortenCustomerSpace(customerSpace));
        List raw = get("getSegments", url, List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    public void deleteSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/customerspaces/{customerSpace}/segments/{segmentName}", //
                shortenCustomerSpace(customerSpace), segmentName);
        delete("deleteSegmentByName", url);
    }
}

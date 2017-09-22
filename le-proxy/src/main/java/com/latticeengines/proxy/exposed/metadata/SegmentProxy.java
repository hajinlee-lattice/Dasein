package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("segmentProxy")
public class SegmentProxy extends MicroserviceRestApiProxy {

    protected SegmentProxy() {
        super("metadata/customerspaces");
    }

    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment metadataSegment) {
        String url = constructUrl("/{customerSpace}/segments", //
                shortenCustomerSpace(customerSpace));
        return post("createOrUpdateSegment", url, metadataSegment, MetadataSegment.class);
    }

    public MetadataSegment getMetadataSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}", //
                shortenCustomerSpace(customerSpace), segmentName);
        return get("getSegment", url, MetadataSegment.class);
    }

    public MetadataSegmentDTO getMetadataSegmentWithPidByName(String customerSpace, String segmentName) {
        String url = constructUrl("/{customerSpace}/segments/pid/{segmentName}", //
                shortenCustomerSpace(customerSpace), segmentName);
        return get("getSegmentWithPid", url, MetadataSegmentDTO.class);
    }

    @SuppressWarnings("rawtypes")
    public List<MetadataSegment> getMetadataSegments(String customerSpace) {
        String url = constructUrl("/{customerSpace}/segments", shortenCustomerSpace(customerSpace));
        List raw = get("getSegments", url, List.class);
        return JsonUtils.convertList(raw, MetadataSegment.class);
    }

    public void deleteSegmentByName(String customerSpace, String segmentName) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}", //
                shortenCustomerSpace(customerSpace), segmentName);
        delete("deleteSegmentByName", url);
    }

    public StatisticsContainer getSegmentStats(String customerSpace, String segmentName, DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/stats?version={version}", //
                shortenCustomerSpace(customerSpace), segmentName, version);
        return get("getSegmentStats", url, StatisticsContainer.class);
    }

    public SimpleBooleanResponse upsertStatsToSegment(String customerSpace, String segmentName, StatisticsContainer
            statisticsContainer) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/stats", //
                shortenCustomerSpace(customerSpace), segmentName);
        return post("upsertStatsToSegment", url, statisticsContainer, SimpleBooleanResponse.class);
    }
}

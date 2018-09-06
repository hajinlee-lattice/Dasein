package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentAndActionDTO;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentDTO;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("segmentProxy")
public class SegmentProxy extends MicroserviceRestApiProxy {

    protected SegmentProxy() {
        super("cdl/customerspaces");
    }

    public MetadataSegment createOrUpdateSegment(String customerSpace, MetadataSegment metadataSegment) {
        String url = constructUrl("/{customerSpace}/segments", //
                shortenCustomerSpace(customerSpace));
        return post("createOrUpdateSegment", url, metadataSegment, MetadataSegment.class);
    }

    public MetadataSegmentAndActionDTO createOrUpdateSegmentAndActionDTO(String customerSpace,
            MetadataSegment metadataSegment) {
        String url = constructUrl("/{customerSpace}/segments/with-action", //
                shortenCustomerSpace(customerSpace));
        return post("createOrUpdateSegment with action", url, metadataSegment, MetadataSegmentAndActionDTO.class);
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

    public Map<BusinessEntity, Long> updateSegmentCounts(String customerSpace, String segmentName) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/counts", //
                shortenCustomerSpace(customerSpace), segmentName);
        @SuppressWarnings("rawtypes")
        Map map = put("updateSegmentCounts", url, null, Map.class);
        return JsonUtils.convertMap(map, BusinessEntity.class, Long.class);
    }

    public void updateSegmentsCounts(String customerSpace) {
        String url = constructUrl("/{customerSpace}/segments/counts", shortenCustomerSpace(customerSpace));
        put("updateAllCounts", url, null, Map.class);
    }

    public StatisticsContainer getSegmentStats(String customerSpace, String segmentName,
            DataCollection.Version version) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/stats?version={version}", //
                shortenCustomerSpace(customerSpace), segmentName, version);
        return get("getSegmentStats", url, StatisticsContainer.class);
    }

    public SimpleBooleanResponse upsertStatsToSegment(String customerSpace, String segmentName,
            StatisticsContainer statisticsContainer) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/stats", //
                shortenCustomerSpace(customerSpace), segmentName);
        return post("upsertStatsToSegment", url, statisticsContainer, SimpleBooleanResponse.class);
    }

    @SuppressWarnings("rawtypes")
    public List<AttributeLookup> findDependingAttributes(String customerSpace, List<MetadataSegment> metadataSegments) {
        String url = constructUrl("/{customerSpace}/segments/attributes", //
                shortenCustomerSpace(customerSpace));
        List raw = post("findDependingAttributes", url, metadataSegments, List.class);
        return JsonUtils.convertList(raw, AttributeLookup.class);
    }

    public Map<String, List<String>> getDependencies(String customerSpace, String segmentName) {
        String url = constructUrl("/{customerSpace}/segments/{segmentName}/dependencies", //
                shortenCustomerSpace(customerSpace), segmentName);
        Map<?, ?> raw = get("getDependencies", url, Map.class);
        Map<String, List<String>> result = new HashMap<>();
        if (MapUtils.isNotEmpty(raw)) {
            @SuppressWarnings("rawtypes")
            Map<String, List> midResult = JsonUtils.convertMap(raw, String.class, List.class);
            midResult.keySet().stream() //
                    .forEach(k -> {
                        List<?> list = midResult.get(k);
                        result.put(k, JsonUtils.convertList(list, String.class));
                    });
        }
        return result;
    }
}

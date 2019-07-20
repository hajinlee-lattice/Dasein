package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;

@Component("bucketedScoreProxy")
public class BucketedScoreProxyImpl extends MicroserviceRestApiProxy implements BucketedScoreProxy {

    protected BucketedScoreProxyImpl() {
        super("lp");
    }

    @Override
    public void createABCDBuckets(String customerSpace, CreateBucketMetadataRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/abcdbuckets",
                shortenCustomerSpace(customerSpace));
        post("create bucket metadata", url, request, SimpleBooleanResponse.class);
    }

    @Override
    public List<BucketMetadata> updateABCDBuckets(String customerSpace, UpdateBucketMetadataRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/abcdbuckets",
                shortenCustomerSpace(customerSpace));
        List<?> list = put("update bucket metadata", url, request, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/abcdbuckets/model/{modelGuid}",
                shortenCustomerSpace(customerSpace), modelGuid);
        Map<?, ?> map = get("get bucket metadata history for model", url, Map.class);
        return parseABCDBucketsHistory(map);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(String customerSpace, String engineId) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/abcdbuckets/engine/{engineId}",
                shortenCustomerSpace(customerSpace), engineId);
        Map<?, ?> map = get("get bucket metadata history for engine", url, Map.class);
        return parseABCDBucketsHistory(map);
    }

    @Override
    public List<BucketMetadata> getLatestABCDBucketsByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/uptodateabcdbuckets/model/{modelGuid}",
                shortenCustomerSpace(customerSpace), modelGuid);
        List<?> list = get("get up-to-date bucket metadata history for model", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public Map<String, List<BucketMetadata>> getAllPublishedBucketMetadataByModelSummaryIdList(String customerSpace,
            List<String> modelSummaryIdList) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/publishedbuckets/model",
                shortenCustomerSpace(customerSpace));
        List<String> params = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(modelSummaryIdList)) {
            for (String id : modelSummaryIdList) {
                params.add("model-summary-id=" + id);
            }
        }
        if (!params.isEmpty()) {
            url += "?" + StringUtils.join(params, "&");
        }
        Map<?, ?> map = get("get bucket metadata list for model ids", url, Map.class);
        return parseIdToBucketsMap(map);
    }

    @Override
    public List<BucketMetadata> getPublishedBucketMetadataByModelGuid(String customerSpace, String modelSummaryId) {
        String url =
                constructUrl("/customerspaces/{customerSpace}/bucketedscore/publishedbuckets/model/{modelSummaryId}",
                        shortenCustomerSpace(customerSpace), modelSummaryId);
        List<?> list = get("get up-to-date bucket metadata history for model", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public List<BucketMetadata> getAllBucketsByEngineId(String customerSpace, String engineId) {
        String url =
                constructUrl("/customerspaces/{customerSpace}/bucketedscore/abcdbuckets/ratingengines/{ratingEngineId}",
                        shortenCustomerSpace(customerSpace), engineId);
        return getList("get bucket metadata history for engine", url, BucketMetadata.class);
    }

    @Override
    public List<BucketMetadata> getAllPublishedBucketsByEngineId(String customerSpace, String engineId) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/bucketedscore/publishedbuckets/ratingengines/{ratingEngineId}",
                shortenCustomerSpace(customerSpace), engineId);
        return getList("get bucket metadata history for engine", url, BucketMetadata.class);
    }

    @Override
    public BucketedScoreSummary getBucketedScoreSummary(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/summary/model/{modelGuid}",
                shortenCustomerSpace(customerSpace), modelGuid);
        return get("get bucketed score summary", url, BucketedScoreSummary.class);
    }

    @Override
    public BucketedScoreSummary createOrUpdateBucketedScoreSummary(String customerSpace, String modelGuid,
            BucketedScoreSummary summary) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/summary/model/{modelGuid}",
                shortenCustomerSpace(customerSpace), modelGuid);
        return post("create bucketed score summary", url, summary, BucketedScoreSummary.class);
    }

    @SuppressWarnings("rawtypes")
    private Map<Long, List<BucketMetadata>> parseABCDBucketsHistory(Map<?, ?> map) {
        if (MapUtils.isNotEmpty(map)) {
            Map<Long, List> listMap = JsonUtils.convertMap(map, Long.class, List.class);
            Map<Long, List<BucketMetadata>> result = new HashMap<>();
            listMap.forEach((k, v) -> result.put(k, JsonUtils.convertList(v, BucketMetadata.class)));
            return result;
        } else {
            return Collections.emptyMap();
        }
    }

    @SuppressWarnings("rawtypes")
    private Map<String, List<BucketMetadata>> parseIdToBucketsMap(Map<?, ?> map) {
        if (MapUtils.isNotEmpty(map)) {
            Map<String, List> listMap = JsonUtils.convertMap(map, String.class, List.class);
            Map<String, List<BucketMetadata>> result = new HashMap<>();
            listMap.forEach((k, v) -> result.put(k, JsonUtils.convertList(v, BucketMetadata.class)));
            return result;
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public List<BucketMetadata> getModelABCDBucketsByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucketedscore/modelabcdbuckets/model/{modelGuid}",
                shortenCustomerSpace(customerSpace), modelGuid);
        List<?> list = get("get model bucket metadata history for model", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

}

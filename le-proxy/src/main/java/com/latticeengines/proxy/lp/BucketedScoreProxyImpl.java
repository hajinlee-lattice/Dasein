package com.latticeengines.proxy.lp;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;


@Component("bucketedScoreProxy")
public class BucketedScoreProxyImpl extends MicroserviceRestApiProxy implements BucketedScoreProxy {

    protected BucketedScoreProxyImpl() {
        super("lp/bucketedscore");
    }

    @Override
    public void createABCDBuckets(CreateBucketMetadataRequest request) {
        String url = constructUrl("/abcdbuckets");
        post("create bucket metadata", url, request, SimpleBooleanResponse.class);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String modelGuid) {
        String url = constructUrl("/abcdbuckets/model/{modelGuid}", modelGuid);
        Map map = get("get bucket metadata history for model", url, Map.class);
        return parseABCDBucketsHistory(map);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(String engineId) {
        String url = constructUrl("/abcdbuckets/engine/{engineId}", engineId);
        Map map = get("get bucket metadata history for engine", url, Map.class);
        return parseABCDBucketsHistory(map);
    }

    @Override
    public List<BucketMetadata> getLatestABCDBucketsByModelGuid(String modelGuid) {
        String url = constructUrl("/uptodateabcdbuckets/model/{modelGuid}", modelGuid);
        List list = get("get up-to-date bucket metadata history for model", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public List<BucketMetadata> getLatestABCDBucketsByEngineId(String engineId) {
        String url = constructUrl("/uptodateabcdbuckets/engine/{engineId}", engineId);
        List list = get("get up-to-date bucket metadata for engine", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public BucketedScoreSummary getBucketedScoreSummary(String modelGuid) {
        String url = constructUrl("/summary/model/{modelGuid}", modelGuid);
        return get("get bucketed score summary", url, BucketedScoreSummary.class);
    }

    @Override
    public BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelGuid, BucketedScoreSummary summary) {
        String url = constructUrl("/summary/model/{modelGuid}", modelGuid);
        return post("create bucketed score summary", url, summary, BucketedScoreSummary.class);
    }

    private Map<Long, List<BucketMetadata>> parseABCDBucketsHistory(Map map) {
        if (MapUtils.isNotEmpty(map)) {
            Map<Long, List> listMap = JsonUtils.convertMap(map, Long.class, List.class);
            Map<Long, List<BucketMetadata>> result = new HashMap<>();
            listMap.forEach((k, v) -> result.put(k, JsonUtils.convertList(v, BucketMetadata.class)));
            return result;
        } else {
            return Collections.emptyMap();
        }
    }

}

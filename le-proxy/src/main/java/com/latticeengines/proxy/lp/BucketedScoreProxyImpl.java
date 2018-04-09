package com.latticeengines.proxy.lp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;


@Component("bucketedScoreProxy")
public class BucketedScoreProxyImpl extends MicroserviceRestApiProxy implements BucketedScoreProxy {

    protected BucketedScoreProxyImpl() {
        super("lp");
    }

    @Override
    public void createABCDBuckets(String customerSpace, CreateBucketMetadataRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucktedscore", shortenCustomerSpace(customerSpace));
        post("create bucket metadata", url, request, SimpleBooleanResponse.class);
    }

    @Override
    public Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucktedscore/abcdbuckets/model/{modelGuid}", //
                shortenCustomerSpace(customerSpace), modelGuid);
        Map map = get("get bucket metadata history for model", url, Map.class);
        return parseABCDBucketsHistory(map);
    }

    @Override
    public List<BucketMetadata> getLatestABCDBucketsByModelGuid(String customerSpace, String modelGuid) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucktedscore/uptodateabcdbuckets/model/{modelGuid}", //
                shortenCustomerSpace(customerSpace), modelGuid);
        List list = get("get up-to-date bucket metadata history for model", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
    }

    @Override
    public List<BucketMetadata> getABCDBucketsByEngineId(String customerSpace, String engineId) {
        String url = constructUrl("/customerspaces/{customerSpace}/bucktedscore/abcdbuckets/engine/{engineId}", //
                shortenCustomerSpace(customerSpace), engineId);
        List list = get("get bucket metadata for engine", url, List.class);
        return JsonUtils.convertList(list, BucketMetadata.class);
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

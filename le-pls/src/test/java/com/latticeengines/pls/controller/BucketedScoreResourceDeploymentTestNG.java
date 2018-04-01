package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.pls.service.impl.BucketedScoreServiceImplDeploymentTestNG;

public class BucketedScoreResourceDeploymentTestNG extends BucketedScoreServiceImplDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreResourceDeploymentTestNG.class);

    @Override
    protected void createBucketMetadatas(String ratingEngineId, String ratingModelId,
            List<BucketMetadata> bucketMetadata) {
        log.info("MultiTenantContext.getEmailAddress() is " + MultiTenantContext.getEmailAddress());
        restTemplate.postForObject(getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/ratingengine/"
                + ratingEngineId + "/model/" + ratingModelId, bucketMetadata, Void.class);
    }

    @Override
    protected Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
            String ratingEngineId) {
        @SuppressWarnings("unchecked")
        Map<?, List<?>> map = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/ratingengine/" + ratingEngineId, Map.class);
        Map<Long, List<BucketMetadata>> convertedMap = JsonUtils.convertMapWithListValue(map, Long.class,
                BucketMetadata.class);
        return convertedMap;
    }

    @Override
    protected List<BucketMetadata> getUpToDateABCDBucketsBasedOnRatingEngineId(String ratingEngineId) {
        List<?> list = restTemplate.getForObject(
                getRestAPIHostPort() + "/pls/bucketedscore/abcdbuckets/uptodate/ratingengine/" + ratingEngineId,
                List.class);
        List<BucketMetadata> returnList = JsonUtils.convertList(list, BucketMetadata.class);
        return returnList;
    }

    @AfterClass(groups = "deployment")
    public void willDelete() {
        throw new RuntimeException("error");
    }
}

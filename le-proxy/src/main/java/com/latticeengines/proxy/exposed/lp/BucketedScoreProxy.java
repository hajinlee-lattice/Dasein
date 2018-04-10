package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public interface BucketedScoreProxy {

    void createABCDBuckets(CreateBucketMetadataRequest request);

    Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String modelGuid);

    Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(String engineId);

    List<BucketMetadata> getLatestABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getLatestABCDBucketsByEngineId(String engineId);

    BucketedScoreSummary getBucketedScoreSummary(String modelGuid);

    BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelGuid, BucketedScoreSummary summary);

}

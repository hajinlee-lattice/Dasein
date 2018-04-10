package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public interface BucketedScoreProxy {

    void createABCDBuckets(CreateBucketMetadataRequest request);

    Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getLatestABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getABCDBucketsByEngineId(String engineId);

    BucketedScoreSummary getBucketedScoreSummary(String modelGuid);

}

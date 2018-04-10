package com.latticeengines.apps.lp.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public interface BucketedScoreService {

    Map<Long, List<BucketMetadata>> getBucketMetadataGroupedByCreationTimes(String modelGuid);

    List<BucketMetadata> getABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getABCDBucketsByRatingEngineId(String ratingEngineId);

    void createABCDBuckets(CreateBucketMetadataRequest request);

    BucketedScoreSummary getBucketedScoreSummaryByModelGuid(String modelGuid);

    BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelGuid,
            BucketedScoreSummary bucketedScoreSummary);
}

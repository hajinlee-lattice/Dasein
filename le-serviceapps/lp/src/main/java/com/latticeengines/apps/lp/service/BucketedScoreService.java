package com.latticeengines.apps.lp.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public interface BucketedScoreService {

    Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelId);

    List<BucketMetadata> getUpToDateModelBucketMetadata(String modelId);

    List<BucketMetadata> getABCDBucketsByRatingEngine(String ratingEngineId);

    void createABCDBucketsForModel(CreateBucketMetadataRequest request);
}

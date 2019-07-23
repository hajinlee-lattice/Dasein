package com.latticeengines.apps.lp.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;

public interface BucketedScoreService {

    Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelGuid);

    Map<Long, List<BucketMetadata>> getRatingEngineBucketMetadataGroupedByCreationTimes(String ratingEngineId);

    List<BucketMetadata> getABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getModelABCDBucketsByModelGuid(String modelGuid);

    List<BucketMetadata> getABCDBucketsByRatingEngineId(String ratingEngineId);

    void createABCDBuckets(CreateBucketMetadataRequest request);

    List<BucketMetadata> updateABCDBuckets(UpdateBucketMetadataRequest request);

    BucketedScoreSummary getBucketedScoreSummaryByModelGuid(String modelGuid);

    BucketedScoreSummary createOrUpdateBucketedScoreSummary(String modelGuid,
            BucketedScoreSummary bucketedScoreSummary);

    List<BucketMetadata> getABCDBucketsByModelGuidAcrossTenant(String modelGuid);

    List<BucketMetadata> getAllBucketsByRatingEngineId(String ratingEngineId);

    List<BucketMetadata> getAllPublishedBucketsByRatingEngineId(String ratingEngineId);

    List<BucketMetadata> getPublishedBucketMetadataByModelGuid(String modelSummaryId);

    Map<String, List<BucketMetadata>> getAllPublishedBucketMetadataByModelSummaryIdList(
            List<String> modelSummaryIdList);

}

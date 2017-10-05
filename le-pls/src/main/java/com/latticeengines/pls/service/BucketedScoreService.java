package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;

public interface BucketedScoreService {

    BucketedScoreSummary getBucketedScoreSummaryForModelId(String modelId) throws Exception;

    Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimes(String modelId);

    void createBucketMetadatas(String modelId, List<BucketMetadata> bucketMetadatas);

    List<BucketMetadata> getUpToDateModelBucketMetadata(String modelId);

    List<BucketMetadata> getUpToDateModelBucketMetadataAcrossTenants(String modelId);

}

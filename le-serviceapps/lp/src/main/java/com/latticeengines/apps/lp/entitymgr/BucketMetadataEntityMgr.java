package com.latticeengines.apps.lp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public interface BucketMetadataEntityMgr extends BaseEntityMgrRepository<BucketMetadata, Long> {

    void createBucketMetadata(List<BucketMetadata> bucketMetadataList, String modelId, String engineId);

    List<BucketMetadata> getBucketMetadatasForModelFromReader(String modelGuid);

    List<BucketMetadata> getBucketMetadatasForEngineFromReader(String engineId);

    List<BucketMetadata> getUpToDateBucketMetadatasForModelFromReader(String modelId);
    
    List<BucketMetadata> getModelBucketMetadatasFromReader(String modelId);

    List<BucketMetadata> getUpToDateBucketMetadatasForEngineFromReader(String engineId);

    BucketMetadata getBucketMetadatasByBucketNameAndTimestamp(String bucketName, long timestamp);

    List<BucketMetadata> getAllBucketMetadatasForEngineFromReader(String engineId);

    List<BucketMetadata> getAllPublishedBucketMetadatasForEngineFromReader(String engineId);

    Integer getMaxPublishedVersionByModelId(String modelId);

    List<BucketMetadata> getPublishedMetadataByModelGuidAndPublishedVersionFromReader(String modelSummaryId,
            Integer publishedVersion);
}

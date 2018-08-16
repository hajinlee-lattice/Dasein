package com.latticeengines.apps.lp.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public interface BucketedMetadataRepository extends BaseJpaRepository<BucketMetadata, Long> {

    List<BucketMetadata> findByRatingEngine_Id(String engineId);

    List<BucketMetadata> findByModelSummary_Id(String modelGuid);

    List<BucketMetadata> findByCreationTimestampAndRatingEngine_Id(long creationTimestamp, String engineId);

    List<BucketMetadata> findByCreationTimestampAndModelSummary_Id(long creationTimestamp, String modelGuid);

    BucketMetadata findFirstByModelSummary_IdOrderByCreationTimestampDesc(String modelGuid);

    BucketMetadata findFirstByRatingEngine_IdOrderByCreationTimestampDesc(String engineId);

    BucketMetadata findByCreationTimestampAndBucketName(long creationTimestamp, BucketName bucketName);

    List<BucketMetadata> findAllFirstByRatingEngine_Id(String engineId);

}

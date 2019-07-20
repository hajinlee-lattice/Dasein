package com.latticeengines.apps.lp.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;

public interface BucketedMetadataRepository extends BaseJpaRepository<BucketMetadata, Long> {

    List<BucketMetadata> findByRatingEngine_Id(String engineId);

    List<BucketMetadata> findByModelSummary_Id(String modelGuid);

    List<BucketMetadata> findByCreationTimestampAndRatingEngine_Id(long creationTimestamp, String engineId);

    @Query("select m from BucketMetadata m where m.modelSummary.id = ?1 and m.creationTimestamp = m.origCreationTimestamp order by m.creationTimestamp Desc")
    List<BucketMetadata> findFirstByModelSummary_IdForModel(String modelGuid, Pageable pageable);
    
    List<BucketMetadata> findByCreationTimestampAndModelSummary_Id(long creationTimestamp, String modelGuid);

    BucketMetadata findFirstByModelSummary_IdOrderByCreationTimestampDesc(String modelGuid);

    BucketMetadata findFirstByRatingEngine_IdOrderByCreationTimestampDesc(String engineId);

    BucketMetadata findByCreationTimestampAndBucketName(long creationTimestamp, BucketName bucketName);

    List<BucketMetadata> findByRatingEngine_IdAndPublishedVersionNotNull(String engineId);

    BucketMetadata findFirstByModelSummary_IdOrderByPublishedVersionDesc(String modelId);

    List<BucketMetadata> findByModelSummary_IdAndPublishedVersion(String modelSummaryId, Integer publishedVersion);
}

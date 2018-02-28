package com.latticeengines.pls.repository;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface BucketMetadataRepository extends BaseJpaRepository<BucketMetadata, Long> {

    public List<BucketMetadata> findByRatingEngine(RatingEngine ratingEngine);

}

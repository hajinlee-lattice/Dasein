package com.latticeengines.datacloud.collection.repository;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

public interface CollectionWorkerRepository extends BaseJpaRepository<CollectionWorker, Long> {
    List<CollectionWorker> findByStatusIn(Collection<String> statuses);
    List<CollectionWorker> findByStatusInAndVendor(Collection<String> statuses, String vendor);
    List<CollectionWorker> findByStatusInAndVendorAndSpawnTimeIsAfter(Collection<String> statuses, String vendor,
                                                                         Timestamp spawnTime);
}

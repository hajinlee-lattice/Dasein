package com.latticeengines.ldc_collectiondb.repository.writer;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionWorker;

public interface CollectionWorkerRepository extends BaseJpaRepository<CollectionWorker, Long> {

    List<CollectionWorker> findByStatusIn(Collection<String> statuses);

    List<CollectionWorker> findByStatusInAndVendor(Collection<String> statuses, String vendor);

    List<CollectionWorker> findByStatusInAndVendorAndSpawnTimeIsAfter(Collection<String> statuses, String vendor,
                                                                      Timestamp spawnTime);

    @Transactional
    @Modifying
    void removeBySpawnTimeBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> findBySpawnTimeBetween(Timestamp start, Timestamp end);

    List<CollectionWorker> findByTerminationTimeIsAfterAndStatus(Timestamp start, String status);
}

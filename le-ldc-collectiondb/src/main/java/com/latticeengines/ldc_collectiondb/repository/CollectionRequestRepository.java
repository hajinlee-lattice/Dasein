package com.latticeengines.ldc_collectiondb.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;

public interface CollectionRequestRepository extends BaseJpaRepository<CollectionRequest, Long> {

    List<CollectionRequest> findByVendorAndDomainIn(String vendor, Collection<String> domains);

    List<CollectionRequest> findByVendorAndPickupWorkerInAndStatusNotIn(String vendor,
            Collection<String> pickupWorkers, Collection<String> statuses);

    List<CollectionRequest> findByPickupWorker(String pickupWorker);

    List<CollectionRequest> findByVendorAndStatusOrderByRequestedTimeAsc(String vendor,
                                                                         String status, Pageable pageable);

    @Query("SELECT MIN(cr.pid) from CollectionRequest cr")
    long getMinPid();
}

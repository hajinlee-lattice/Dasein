package com.latticeengines.ldc_collectiondb.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;

public interface CollectionRequestRepository extends BaseJpaRepository<CollectionRequest, Long> {

    List<CollectionRequest> findByVendorAndDomainIn(String vendor, Collection<String> domains);

    List<CollectionRequest> findByVendorAndStatusIn(String vendor,
                                                    Collection<String> statuses);

    List<CollectionRequest> findByVendorAndStatusInAndPickupWorkerNotIn(String vendor,
                                                                        Collection<String> statuses,
                                                                        Collection<String> pickupWorkers);

    List<CollectionRequest> findByPickupWorker(String pickupWorker);

    List<CollectionRequest> findByVendorAndStatusOrderByRequestedTimeAsc(String vendor,
                                                                         String status, Pageable pageable);

    @Query("SELECT MIN(t.pid) from CollectionRequest t where t.vendor = ?1")
    Long getMinPid(String vendor);
}

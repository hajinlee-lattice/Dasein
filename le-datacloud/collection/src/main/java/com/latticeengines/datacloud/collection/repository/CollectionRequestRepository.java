package com.latticeengines.datacloud.collection.repository;

import java.util.Collection;
import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.CollectionRequest;

public interface CollectionRequestRepository extends BaseJpaRepository<CollectionRequest, Long> {
    List<CollectionRequest> findByVendorAndDomainIn(String vendor, Collection<String> domains);
    List<CollectionRequest> findByVendorAndPickupWorkerInAndStatusNotIn(String vendor,
                                                                        Collection<String> pickupWorkers,
                                                                        Collection<String> statuses);
    List<CollectionRequest> findByPickupWorker(String pickupWorker);
    List<CollectionRequest> findByVendorAndStatusOrderByRequestedTimeAsc(String vendor, String status);
}

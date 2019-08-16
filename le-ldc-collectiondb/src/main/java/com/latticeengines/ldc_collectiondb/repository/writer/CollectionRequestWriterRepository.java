package com.latticeengines.ldc_collectiondb.repository.writer;

import java.sql.Timestamp;
import java.util.Collection;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.ldc_collectiondb.repository.CollectionRequestRepository;

public interface CollectionRequestWriterRepository extends CollectionRequestRepository {

    @Transactional
    @Modifying
    void removeByRequestedTimeBetween(Timestamp start, Timestamp end);

    @Modifying
    @Transactional
    void removeByStatusInAndVendorAndDeliveryTimeBeforeAndPidLessThan(Collection<String> statuses, String vendor, Timestamp before, Long pid);
}

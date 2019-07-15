package com.latticeengines.ldc_collectiondb.repository.writer;

import java.sql.Timestamp;


import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.ldc_collectiondb.repository.RawCollectionRequestRepository;

public interface RawCollectionRequestWriterRepository extends RawCollectionRequestRepository {
    @Modifying
    @Transactional
    void removeByRequestedTimeBetween(Timestamp start, Timestamp end);

    @Modifying
    @Transactional
    void removeByTransferred(Boolean transferred);
}

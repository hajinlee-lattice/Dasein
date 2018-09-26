package com.latticeengines.ldc_collectiondb.repository.reader;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.RawCollectionRequest;

public interface RawCollectionRequestReaderRepository extends BaseJpaRepository<RawCollectionRequest, Long> {
    List<RawCollectionRequest> findByTransferred(boolean transferred);
    List<RawCollectionRequest> findByTransferredAndVendor(boolean transferred, String vendor);
}

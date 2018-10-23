package com.latticeengines.ldc_collectiondb.repository.writer;

import java.util.List;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;

public interface VendorConfigRepository extends BaseJpaRepository<VendorConfig, Long> {

    List<VendorConfig> findByCollectorEnabled(boolean enabled);

    VendorConfig findByVendor(String vendor);

}

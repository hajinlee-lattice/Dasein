package com.latticeengines.ldc_collectiondb.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;

import java.util.List;

public interface VendorConfigMgr extends BaseEntityMgrRepository<VendorConfig, Long> {

    List<VendorConfig> getEnabledVendors();

    VendorConfig getVendorConfig(String vendor);

}

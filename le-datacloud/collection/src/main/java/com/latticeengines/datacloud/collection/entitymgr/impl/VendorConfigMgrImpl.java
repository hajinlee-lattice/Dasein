package com.latticeengines.datacloud.collection.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.VendorConfigMgr;
import com.latticeengines.datacloud.collection.repository.VendorConfigRepository;
import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;

@Component
public class VendorConfigMgrImpl extends JpaEntityMgrRepositoryImpl<VendorConfig, Long> implements VendorConfigMgr {
    private static final String VENDOR_ALEXA = "ALEXA";
    private static final String VENDOR_BUILTWITH = "BUILTWITH";
    private static final String VENDOR_COMPETE = "COMPETE";
    private static final String VENDOR_FEATURE = "FEATURE";
    private static final String VENDOR_HPA_NEW = "HPA_NEW";
    private static final String VENDOR_ORBI_V2 = "ORBINTELLIGENCEV2";
    private static final String VENDOR_SEMRUSH = "SEMRUSH";
    private static final Logger log = LoggerFactory.getLogger(VendorConfigMgrImpl.class);

    @Inject
    private VendorConfigRepository vendorConfigRepository;

    @Override
    public BaseJpaRepository<VendorConfig, Long> getRepository() {
        return vendorConfigRepository;
    }
}

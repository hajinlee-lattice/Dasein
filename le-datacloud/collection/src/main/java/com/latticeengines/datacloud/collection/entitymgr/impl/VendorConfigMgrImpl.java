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
    @SuppressWarnings("unused")
    private static final String VENDOR_ALEXA = "ALEXA";
    @SuppressWarnings("unused")
    private static final String VENDOR_BUILTWITH = "BUILTWITH";
    @SuppressWarnings("unused")
    private static final String VENDOR_COMPETE = "COMPETE";
    @SuppressWarnings("unused")
    private static final String VENDOR_FEATURE = "FEATURE";
    @SuppressWarnings("unused")
    private static final String VENDOR_HPA_NEW = "HPA_NEW";
    @SuppressWarnings("unused")
    private static final String VENDOR_ORBI_V2 = "ORBINTELLIGENCEV2";
    @SuppressWarnings("unused")
    private static final String VENDOR_SEMRUSH = "SEMRUSH";
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(VendorConfigMgrImpl.class);

    @Inject
    private VendorConfigRepository vendorConfigRepository;

    @Override
    public BaseJpaRepository<VendorConfig, Long> getRepository() {
        return vendorConfigRepository;
    }
}

package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;
import com.latticeengines.ldc_collectiondb.repository.writer.VendorConfigRepository;

@Component
public class VendorConfigMgrImpl extends JpaEntityMgrRepositoryImpl<VendorConfig, Long> implements VendorConfigMgr {
    private static final Logger log = LoggerFactory.getLogger(VendorConfigMgrImpl.class);

    @Inject
    private VendorConfigRepository vendorConfigRepository;

    @Override
    public BaseJpaRepository<VendorConfig, Long> getRepository() {

        return vendorConfigRepository;

    }

}

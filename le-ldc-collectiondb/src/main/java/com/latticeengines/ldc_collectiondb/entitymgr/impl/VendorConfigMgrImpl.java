package com.latticeengines.ldc_collectiondb.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.entitymgr.impl.JpaEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.ldc_collectiondb.entity.VendorConfig;
import com.latticeengines.ldc_collectiondb.entitymgr.VendorConfigMgr;
import com.latticeengines.ldc_collectiondb.repository.writer.VendorConfigRepository;

@Component
public class VendorConfigMgrImpl extends JpaEntityMgrRepositoryImpl<VendorConfig, Long> implements VendorConfigMgr {

    @Inject
    private VendorConfigRepository repository;

    @Override
    public BaseJpaRepository<VendorConfig, Long> getRepository() {
        return repository;
    }

    @Override
    public List<VendorConfig> getEnabledVendors() {
        return repository.findByCollectorEnabled(true);
    }

    @Override
    public VendorConfig getVendorConfig(String vendor) {
        return repository.findByVendor(vendor);
    }

}

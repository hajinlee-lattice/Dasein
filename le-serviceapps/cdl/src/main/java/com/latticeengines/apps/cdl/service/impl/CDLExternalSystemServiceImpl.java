package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("cdlExternalSystemService")
public class CDLExternalSystemServiceImpl implements CDLExternalSystemService {

    @Autowired
    private CDLExternalSystemEntityMgr cdlExternalSystemEntityMgr;

    @Override
    public List<CDLExternalSystem> getAllExternalSystem(String customerSpace) {
        return cdlExternalSystemEntityMgr.findAllExternalSystem();
    }

    @Override
    public CDLExternalSystem getExternalSystem(String customerSpace) {
        return cdlExternalSystemEntityMgr.findExternalSystem();
    }

    @Override
    public void createOrUpdateExternalSystem(String customerSpace, CDLExternalSystem cdlExternalSystem) {
        CDLExternalSystem existingSystem = cdlExternalSystemEntityMgr.findExternalSystem();
        if (existingSystem == null) {
            cdlExternalSystem.setTenant(MultiTenantContext.getTenant());
            cdlExternalSystemEntityMgr.create(cdlExternalSystem);
        } else {
            existingSystem.setCrmIds(cdlExternalSystem.getCrmIds());
            existingSystem.setErpIds(cdlExternalSystem.getErpIds());
            existingSystem.setMapIds(cdlExternalSystem.getMapIds());
            existingSystem.setOtherIds(cdlExternalSystem.getOtherIds());
            cdlExternalSystemEntityMgr.update(existingSystem);
        }
    }
}

package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.CDLExternalSystemEntityMgr;
import com.latticeengines.apps.cdl.service.CDLExternalSystemService;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
    public void createExternalSystem(String customerSpace, String systemType, InterfaceName accountInterface) {
        CDLExternalSystem.CRMType crm = null;
        CDLExternalSystem.MAPType map = null;
        CDLExternalSystem.ERPType erp = null;
        switch (systemType.toUpperCase()) {
            case "CRM":
                crm = CDLExternalSystem.CRMType.fromAccountInterface(accountInterface);
                break;
            case "MAP":
                map = CDLExternalSystem.MAPType.fromAccountInterface(accountInterface);
                break;
            case "ERP":
                erp = CDLExternalSystem.ERPType.fromAccountInterface(accountInterface);
                break;
            default:
                throw new RuntimeException("Cannot recognize system type: " + systemType);
        }
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        cdlExternalSystem.setTenant(MultiTenantContext.getTenant());
        cdlExternalSystem.setCRM(crm);
        cdlExternalSystem.setMAP(map);
        cdlExternalSystem.setERP(erp);
        cdlExternalSystemEntityMgr.create(cdlExternalSystem);
    }

    @Override
    public void createExternalSystem(String customerSpace, InterfaceName crmAccountInt, InterfaceName mapAccountInt, InterfaceName erpAccountInt) {
        CDLExternalSystem.CRMType crm = CDLExternalSystem.CRMType.fromAccountInterface(crmAccountInt);
        CDLExternalSystem.MAPType map = CDLExternalSystem.MAPType.fromAccountInterface(mapAccountInt);
        CDLExternalSystem.ERPType erp = CDLExternalSystem.ERPType.fromAccountInterface(erpAccountInt);
        CDLExternalSystem cdlExternalSystem = new CDLExternalSystem();
        cdlExternalSystem.setTenant(MultiTenantContext.getTenant());
        cdlExternalSystem.setCRM(crm);
        cdlExternalSystem.setMAP(map);
        cdlExternalSystem.setERP(erp);
        cdlExternalSystemEntityMgr.create(cdlExternalSystem);
    }
}

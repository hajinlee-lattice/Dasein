package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.ProspectDiscoveryOptionDao;
import com.latticeengines.pls.entitymanager.ProspectDiscoveryOptionEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("prospectDiscoveryOptionEntityMgr")
public class ProspectDiscoveryOptionEntityMgrImpl extends BaseEntityMgrImpl<ProspectDiscoveryOption> implements
        ProspectDiscoveryOptionEntityMgr {

    @Autowired
    private ProspectDiscoveryOptionDao prospectDiscoveryDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;
    
    @Override
    public BaseDao<ProspectDiscoveryOption> getDao() {
        return this.prospectDiscoveryDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(ProspectDiscoveryOption prospectDiscoveryOption) {
        if (this.findProspectDiscoveryOption(prospectDiscoveryOption.getOption()) != null) {
            throw new RuntimeException(String.format("Prospect Discovery Option for tenant: %s already exists", MultiTenantContext.getTenant().getName()));
        }
        setTenantIdOnProspectDiscoveryOption(prospectDiscoveryOption);
        this.prospectDiscoveryDao.create(prospectDiscoveryOption);
    }
    
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ProspectDiscoveryOption> findAllProspectDiscoveryOptions() {
        return this.prospectDiscoveryDao.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ProspectDiscoveryOption findProspectDiscoveryOption(String option) {
        return this.prospectDiscoveryDao.findProspectDiscoveryOption(option);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteProspectDiscoveryOption(String option) {
        ProspectDiscoveryOption prospectDiscoveryOption = this.prospectDiscoveryDao.findProspectDiscoveryOption(option);
        this.prospectDiscoveryDao.delete(prospectDiscoveryOption);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateProspectDiscoveryOption(String option, String value) {
        if (this.findProspectDiscoveryOption(option) != null) {
            this.deleteProspectDiscoveryOption(option);
        }
        ProspectDiscoveryOption prospectDiscoveryOption = new ProspectDiscoveryOption();
        prospectDiscoveryOption.setOption(option);
        prospectDiscoveryOption.setValue(value);
        this.create(prospectDiscoveryOption);
    }

    private void setTenantIdOnProspectDiscoveryOption(ProspectDiscoveryOption prospectDiscoveryOption) {
        Tenant tenant = this.tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        prospectDiscoveryOption.setTenantId(tenant.getPid());
    }
}

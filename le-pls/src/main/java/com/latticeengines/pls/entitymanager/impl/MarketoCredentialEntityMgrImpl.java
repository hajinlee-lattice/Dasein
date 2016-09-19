package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.MarketoCredentialDao;
import com.latticeengines.pls.entitymanager.EnrichmentEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("marketoCredentialEntityMgr")
public class MarketoCredentialEntityMgrImpl extends BaseEntityMgrImpl<MarketoCredential>
        implements MarketoCredentialEntityMgr {

    @Autowired
    private MarketoCredentialDao marketoCredentialDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private EnrichmentEntityMgr enrichmentEntityMgr;

    @Override
    public BaseDao<MarketoCredential> getDao() {
        return marketoCredentialDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void create(MarketoCredential marketoCredential) {
        populateMarketoCredentialWithTenant(marketoCredential);
        marketoCredential.setEnrichment(enrichmentEntityMgr.createEnrichment());
        marketoCredentialDao.create(marketoCredential);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateMarketoCredentialById(String credentialId,
            MarketoCredential marketoCredential) {
        System.out.println(String.format("credential id is: %s", credentialId));
        if (marketoCredentialDao.findMarketoCredentialById(credentialId) != null) {
            System.out.println(String.format("Found credential"));
            deleteMarketoCredentialById(credentialId);
        }
        create(marketoCredential);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteMarketoCredentialById(String credentialId) {
        MarketoCredential marketoCredential = marketoCredentialDao
                .findMarketoCredentialById(credentialId);
        marketoCredentialDao.deleteMarketoCredentialById(credentialId);
        enrichmentEntityMgr.delete(marketoCredential.getEnrichment());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MarketoCredential findMarketoCredentialById(String credentialId) {
        return marketoCredentialDao.findMarketoCredentialById(credentialId);
    }

    private void populateMarketoCredentialWithTenant(MarketoCredential marketoCredential) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoCredential.setTenant(tenant);
        marketoCredential.setTenantId(tenant.getPid());
        marketoCredential.setPid(null);
    }

}

package com.latticeengines.saml.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.saml.IdentityProvider;
import com.latticeengines.saml.dao.IdentityProviderDao;
import com.latticeengines.saml.entitymgr.IdentityProviderEntityMgr;

@Component("identityProviderEntityMgr")
public class IdentityProviderEntityMgrImpl extends BaseEntityMgrImpl<IdentityProvider> implements
        IdentityProviderEntityMgr {
    @Autowired
    private IdentityProviderDao identityProviderDao;

    @Override
    public BaseDao<IdentityProvider> getDao() {
        return identityProviderDao;
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void create(IdentityProvider identityProvider) {
        IdentityProvider existing = findByEntityId(identityProvider.getEntityId());
        if (existing != null) {
            throw new LedpException(LedpCode.LEDP_33000, new String[] { identityProvider.getEntityId() });
        }

        super.create(identityProvider);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRED)
    public void delete(IdentityProvider identityProvider) {
        super.delete(identityProvider);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<IdentityProvider> findAll() {
        return getDao().findAll();
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<IdentityProvider> findByTenantId(String tenantId) {
        return identityProviderDao.findByTenantId(tenantId);
    }

    @Override
    @Transactional(value = "globalAuth", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public IdentityProvider findByEntityId(String entityId) {
        return identityProviderDao.findByField("entityId", entityId);
    }
}

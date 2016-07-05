package com.latticeengines.saml.entitymgr.impl;

import java.util.List;

import org.opensaml.saml2.metadata.EntityDescriptor;
import org.opensaml.xml.parse.ParserPool;
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
import com.latticeengines.saml.util.SAMLUtils;

@Component("identityProviderEntityMgr")
public class IdentityProviderEntityMgrImpl extends BaseEntityMgrImpl<IdentityProvider> implements
        IdentityProviderEntityMgr {
    @Autowired
    private IdentityProviderDao identityProviderDao;

    @Autowired
    private ParserPool parserPool;

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
        validate(identityProvider);

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

    private void validate(IdentityProvider identityProvider) {
        try {
            EntityDescriptor descriptor = (EntityDescriptor) SAMLUtils.deserialize(parserPool,
                    identityProvider.getMetadata());

            if (!descriptor.getEntityID().equals(identityProvider.getEntityId())) {
                throw new RuntimeException("Entity ID in XML does not match provided Entity ID");
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_33000,
                    new String[] { identityProvider.getEntityId(), e.getMessage() });
        }

    }
}

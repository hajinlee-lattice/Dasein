package com.latticeengines.pls.entitymanager.impl;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.MarketoCredential;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.MarketoCredentialDao;
import com.latticeengines.pls.entitymanager.EnrichmentEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoCredentialEntityMgr;

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
        if (marketoCredentialDao.findByField("NAME", marketoCredential.getName()) != null) {
            throw new LedpException(LedpCode.LEDP_18119, new String[] { marketoCredential.getName() });
        }
        populateMarketoCredentialWithTenant(marketoCredential);
        marketoCredential.setEnrichment(enrichmentEntityMgr.createEnrichment());
        marketoCredential.setLatticeSecretKey(UUID.randomUUID().toString());
        marketoCredentialDao.create(marketoCredential);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateMarketoCredentialById(String credentialId, MarketoCredential marketoCredential) {
        if (marketoCredentialDao.findByField("NAME", marketoCredential.getName()) != null) {
            throw new LedpException(LedpCode.LEDP_18119, new String[] { marketoCredential.getName() });
        }
        MarketoCredential marketoCredential1 = marketoCredentialDao.findMarketoCredentialById(credentialId);

        marketoCredential1.setName(marketoCredential.getName());
        marketoCredential1.setRestClientId(marketoCredential.getRestClientId());
        marketoCredential1.setRestClientSecret(marketoCredential.getRestClientSecret());
        marketoCredential1.setRestIdentityEnpoint(marketoCredential.getRestIdentityEnpoint());
        marketoCredential1.setRestEndpoint(marketoCredential.getRestEndpoint());
        marketoCredential1.setSoapEndpoint(marketoCredential.getSoapEndpoint());
        marketoCredential1.setSoapUserId(marketoCredential.getSoapUserId());
        marketoCredential1.setSoapEncryptionKey(marketoCredential.getSoapEncryptionKey());
        if (StringUtils.isNotBlank(marketoCredential.getLatticeSecretKey())) {
            marketoCredential1.setLatticeSecretKey(marketoCredential.getLatticeSecretKey().trim());
        } else {
            marketoCredential1.setLatticeSecretKey(UUID.randomUUID().toString());
        }
        marketoCredentialDao.update(marketoCredential1);

    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteMarketoCredentialById(String credentialId) {
        MarketoCredential marketoCredential = marketoCredentialDao.findMarketoCredentialById(credentialId);
        marketoCredentialDao.deleteMarketoCredentialById(credentialId);
        enrichmentEntityMgr.delete(marketoCredential.getEnrichment());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public MarketoCredential findMarketoCredentialById(String credentialId) {
        return marketoCredentialDao.findMarketoCredentialById(credentialId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MarketoCredential> findAllMarketoCredentials() {
        return marketoCredentialDao.findAll();
    }

    private void populateMarketoCredentialWithTenant(MarketoCredential marketoCredential) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoCredential.setTenant(tenant);
        marketoCredential.setTenantId(tenant.getPid());
        marketoCredential.setPid(null);
    }

}

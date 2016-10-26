package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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

    @Value("${pls.marketo.enrichment.webhook.url}")
    private String enrichmentWebhookUrl;

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
        assertCredentialNameNotInDatabase(marketoCredential.getName());
        populateMarketoCredentialWithTenant(marketoCredential);
        marketoCredential.setEnrichment(enrichmentEntityMgr.createEnrichment());
        marketoCredentialDao.create(marketoCredential);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateMarketoCredentialById(String credentialId,
            MarketoCredential marketoCredential) {
        assertCredentialNameNotInDatabase(marketoCredential.getName());
        MarketoCredential marketoCredential1 = marketoCredentialDao
                .findMarketoCredentialById(credentialId);

        marketoCredential1.setName(marketoCredential.getName());
        marketoCredential1.setRestClientId(marketoCredential.getRestClientId());
        marketoCredential1.setRestClientSecret(marketoCredential.getRestClientSecret());
        marketoCredential1.setRestIdentityEnpoint(marketoCredential.getRestIdentityEnpoint());
        marketoCredential1.setRestEndpoint(marketoCredential.getRestEndpoint());
        marketoCredential1.setSoapEndpoint(marketoCredential.getSoapEndpoint());
        marketoCredential1.setSoapUserId(marketoCredential.getSoapUserId());
        marketoCredential1.setSoapEncryptionKey(marketoCredential.getSoapEncryptionKey());

        marketoCredentialDao.update(marketoCredential1);
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
        MarketoCredential marketoCredential = marketoCredentialDao
                .findMarketoCredentialById(credentialId);

        marketoCredential.getEnrichment().setWebhookUrl(enrichmentWebhookUrl);
        marketoCredential.getEnrichment().setTenantCredentialGUID(
                UuidUtils.packUuid(MultiTenantContext.getTenant().getId(), credentialId));

        return marketoCredential;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<MarketoCredential> findAllMarketoCredentials() {
        List<MarketoCredential> marketoCredentials = marketoCredentialDao.findAll();

        for (MarketoCredential marketoCredential : marketoCredentials) {
            marketoCredential.getEnrichment().setWebhookUrl(enrichmentWebhookUrl);
            marketoCredential.getEnrichment().setTenantCredentialGUID(
                    UuidUtils.packUuid(MultiTenantContext.getTenant().getId(),
                            Long.toString(marketoCredential.getPid())));
        }

        return marketoCredentials;
    }

    private void assertCredentialNameNotInDatabase(String credentialName) {
        if (marketoCredentialDao.findByField("name", credentialName) != null) {
            throw new LedpException(LedpCode.LEDP_18119, new String[] { credentialName });
        }
    }

    private void populateMarketoCredentialWithTenant(MarketoCredential marketoCredential) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoCredential.setTenant(tenant);
        marketoCredential.setTenantId(tenant.getPid());
        marketoCredential.setPid(null);
    }

}

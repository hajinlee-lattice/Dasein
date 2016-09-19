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
        populateMarketoCredentialWithTenant(marketoCredential);
        marketoCredential.setEnrichment(enrichmentEntityMgr.createEnrichment());
        marketoCredentialDao.create(marketoCredential);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateMarketoCredentialById(String credentialId,
            MarketoCredential marketoCredential) {
        if (marketoCredentialDao.findMarketoCredentialById(credentialId) != null) {
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

    private void populateMarketoCredentialWithTenant(MarketoCredential marketoCredential) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoCredential.setTenant(tenant);
        marketoCredential.setTenantId(tenant.getPid());
        marketoCredential.setPid(null);
    }

}

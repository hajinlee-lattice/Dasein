package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.EnrichmentDao;
import com.latticeengines.pls.entitymanager.EnrichmentEntityMgr;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;

@Component("enrichmentEntityMgr")
public class EnrichmentEntityMgrImpl extends BaseEntityMgrImpl<Enrichment>
        implements EnrichmentEntityMgr {

    @Autowired
    private EnrichmentDao enrichmentDao;

    @Autowired
    private MarketoMatchFieldEntityMgr marketoMatchFieldEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<Enrichment> getDao() {
        return enrichmentDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Enrichment createEnrichment() {
        Enrichment enrichment = new Enrichment();

        populateEnrichmentWithTenant(enrichment);
        enrichmentDao.create(enrichment);
        MarketoMatchField domainMatchField = marketoMatchFieldEntityMgr
                .createMatchFieldWithNameValueAndEnrichment(MarketoMatchFieldName.Domain, null, enrichment);
        MarketoMatchField companyMatchField = marketoMatchFieldEntityMgr
                .createMatchFieldWithNameValueAndEnrichment(MarketoMatchFieldName.Company, null, enrichment);
        MarketoMatchField stateMatchField = marketoMatchFieldEntityMgr
                .createMatchFieldWithNameValueAndEnrichment(MarketoMatchFieldName.State, null, enrichment);
        MarketoMatchField countryMatchField = marketoMatchFieldEntityMgr
                .createMatchFieldWithNameValueAndEnrichment(MarketoMatchFieldName.Country, null, enrichment);
        MarketoMatchField dunsMatchField = marketoMatchFieldEntityMgr
                .createMatchFieldWithNameValueAndEnrichment(MarketoMatchFieldName.DUNS, null, enrichment);
        enrichment.addMarketoMatchField(domainMatchField);
        enrichment.addMarketoMatchField(companyMatchField);
        enrichment.addMarketoMatchField(stateMatchField);
        enrichment.addMarketoMatchField(countryMatchField);
        enrichment.addMarketoMatchField(dunsMatchField);

        return enrichment;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteEnrichmentById(String enrichmentId) {
        enrichmentDao.deleteEnrichmentById(enrichmentId);
    }

    private void populateEnrichmentWithTenant(Enrichment enrichment) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        enrichment.setTenant(tenant);
        enrichment.setTenantId(tenant.getPid());
        enrichment.setPid(null);
    }

}

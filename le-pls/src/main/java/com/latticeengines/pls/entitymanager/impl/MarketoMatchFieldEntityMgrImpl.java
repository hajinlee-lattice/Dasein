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
import com.latticeengines.pls.dao.MarketoMatchFieldDao;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;

@Component("marketoMatchFieldEntityMgr")
public class MarketoMatchFieldEntityMgrImpl extends BaseEntityMgrImpl<MarketoMatchField>
        implements MarketoMatchFieldEntityMgr {

    @Autowired
    private MarketoMatchFieldDao marketoMatchFieldDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<MarketoMatchField> getDao() {
        return marketoMatchFieldDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public MarketoMatchField createMatchFieldWithNameValueAndEnrichment(
            MarketoMatchFieldName marketoMatchFieldName, String marketoValue,
            Enrichment enrichment) {
        MarketoMatchField marketoMatchField = new MarketoMatchField();

        marketoMatchField.setMarketoMatchFieldName(marketoMatchFieldName);
        marketoMatchField.setMarketoFieldName(marketoValue);
        populateMatchFieldWithTenant(marketoMatchField);
        marketoMatchField.setEnrichment(enrichment);
        marketoMatchFieldDao.create(marketoMatchField);

        return marketoMatchField;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateMarketoMatchFieldValue(MarketoMatchFieldName matchFieldName,
            String marketoValue, Enrichment enrichment) {
        marketoMatchFieldDao.deleteMarketoMatchField(matchFieldName, enrichment);
        createMatchFieldWithNameValueAndEnrichment(matchFieldName, marketoValue, enrichment);
    }

    private void populateMatchFieldWithTenant(MarketoMatchField marketoMatchField) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoMatchField.setTenant(tenant);
        marketoMatchField.setTenantId(tenant.getPid());
        marketoMatchField.setPid(null);
    }

}

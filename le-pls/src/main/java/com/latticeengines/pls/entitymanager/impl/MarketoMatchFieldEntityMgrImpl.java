package com.latticeengines.pls.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.domain.exposed.pls.MarketoMatchField;
import com.latticeengines.domain.exposed.pls.MarketoMatchFieldName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.MarketoMatchFieldDao;
import com.latticeengines.pls.entitymanager.MarketoMatchFieldEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

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
    public MarketoMatchField createMatchFieldWithNameAndEnrichment(
            MarketoMatchFieldName marketoMatchFieldName, Enrichment enrichment) {
        MarketoMatchField marketoMatchField = new MarketoMatchField();

        marketoMatchField.setMarketoMatchFieldName(marketoMatchFieldName);
        populateMatchFieldWithTenant(marketoMatchField);
        marketoMatchField.setEnrichment(enrichment);
        marketoMatchFieldDao.create(marketoMatchField);

        return marketoMatchField;
    }

    private void populateMatchFieldWithTenant(MarketoMatchField marketoMatchField) {
        Tenant tenant = tenantEntityMgr.findByTenantId(MultiTenantContext.getTenant().getId());
        marketoMatchField.setTenant(tenant);
        marketoMatchField.setTenantId(tenant.getPid());
        marketoMatchField.setPid(null);
    }

}

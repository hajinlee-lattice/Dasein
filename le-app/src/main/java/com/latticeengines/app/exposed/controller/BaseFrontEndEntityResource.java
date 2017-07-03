package com.latticeengines.app.exposed.controller;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.QueryDecorator;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public abstract class BaseFrontEndEntityResource {

    @Autowired
    protected EntityProxy entityProxy;

    protected abstract QueryDecorator getQueryDecorator();

    public long getCount(BusinessEntity businessEntity, FrontEndQuery frontEndQuery) {
        Query query = QueryTranslator.translate(frontEndQuery);
        query.addLookup(new EntityLookup(businessEntity));
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), query);
    }

    public long getCountForRestriction(BusinessEntity businessEntity, FrontEndRestriction restriction) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setRestriction(restriction);
        Query query = QueryTranslator.translate(frontEndQuery);
        query.addLookup(new EntityLookup(businessEntity));
        return entityProxy.getCount(MultiTenantContext.getCustomerSpace().toString(), query);
    }

    public DataPage getData(FrontEndQuery frontEndQuery) {
        Query query = QueryTranslator.translate(frontEndQuery, getQueryDecorator());
        return entityProxy.getData(MultiTenantContext.getCustomerSpace().toString(), query);
    }

}

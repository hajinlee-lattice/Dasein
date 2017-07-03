package com.latticeengines.domain.exposed.query.frontend;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class QueryDecorator {

    public abstract BusinessEntity getLookupEntity();

    public abstract String[] getEntityLookups();

    public abstract String[] getLDCLookups();

    public abstract BusinessEntity getFreeTextSearchEntity();

    public abstract String[] getFreeTextSearchAttrs();

}

package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class QueryDecorator {

    public abstract boolean addSelects();

    public abstract BusinessEntity getLookupEntity();

    public abstract String[] getEntityLookups();

    public abstract BusinessEntity getFreeTextSearchEntity();

    public abstract String[] getFreeTextSearchAttrs();

}

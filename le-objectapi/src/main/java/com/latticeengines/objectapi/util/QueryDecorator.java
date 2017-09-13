package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class QueryDecorator {

    public abstract boolean addSelects();

    public abstract AttributeLookup[] getAttributeLookups();

    public abstract BusinessEntity getFreeTextSearchEntity();

    public abstract String[] getFreeTextSearchAttrs();

}

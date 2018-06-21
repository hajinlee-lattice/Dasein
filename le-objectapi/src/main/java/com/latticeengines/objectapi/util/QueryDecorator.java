package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public abstract class QueryDecorator {

    public abstract AttributeLookup getIdLookup();

    public abstract boolean isDataQuery();

    public abstract BusinessEntity getFreeTextSearchEntity();

    public abstract AttributeLookup[] getFreeTextSearchAttrs();

}

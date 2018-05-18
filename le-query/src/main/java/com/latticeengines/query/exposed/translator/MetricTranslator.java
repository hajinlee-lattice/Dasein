package com.latticeengines.query.exposed.translator;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.MetricRestriction;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.SubQuery;

public class MetricTranslator {

    public static Restriction convert(MetricRestriction metricRestriction) {
        SubQuery subQuery = constructSubQuery(metricRestriction);
        return Restriction.builder() //
                .let(BusinessEntity.Account, InterfaceName.AccountId.name()) //
                .inSubquery(subQuery) //
                .build();
    }

    private static SubQuery constructSubQuery(MetricRestriction metricRestriction) {
        SubQuery subQuery = new SubQuery();
        Query metricQuery = Query.builder() //
                .select(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name())) //
                .from(BusinessEntity.Account) //
                .where(metricRestriction.getRestriction()) //
                .build();
        subQuery.setQuery(metricQuery);
        return subQuery;
    }

}

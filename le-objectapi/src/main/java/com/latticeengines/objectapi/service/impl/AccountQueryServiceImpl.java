package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.QueryBuilder;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.objectapi.service.AccountQueryService;

@Component("accountQueryService")
public class AccountQueryServiceImpl implements AccountQueryService {

    @Override
    public Query generateAccountQuery(String start, int offset, int pageSize, boolean hasSfdcAccountId,
            DataRequest dataRequest) {
        if (dataRequest == null) {
            dataRequest = new DataRequest();
        }
        long lastModifiedTime = DateTimeUtils.convertToLongUTCISO8601(start);

        List<Restriction> restrictions = new ArrayList<>();
        Restriction lastModifiedRestriction = Restriction.builder().let(BusinessEntity.Account, "lastmodified")
                .gte(lastModifiedTime).build();
        restrictions.add(lastModifiedRestriction);

        if (CollectionUtils.isNotEmpty(dataRequest.getAccountIds())) {
            RestrictionBuilder accoundIdRestrictionBuilder = Restriction.builder();
            RestrictionBuilder[] accountIdRestrictions = dataRequest.getAccountIds().stream()
                    .map(id -> Restriction.builder().let(BusinessEntity.Account, "accountid").eq(id))
                    .toArray(RestrictionBuilder[]::new);
            accoundIdRestrictionBuilder.or(accountIdRestrictions);
            restrictions.add(accoundIdRestrictionBuilder.build());
        }

        // TODO hasSfdcAccountId - need to complete null support; currently not working in le-query
        Restriction restriction = Restriction.builder().and(restrictions).build();

        QueryBuilder queryBuilder = Query.builder();
        if (CollectionUtils.isNotEmpty(dataRequest.getAttributes())) {
            queryBuilder = queryBuilder.select(BusinessEntity.Account,
                    dataRequest.getAttributes().toArray(new String[] {}));
        }
        queryBuilder.select(BusinessEntity.Account, "accountid", "latticeaccountid", "salesforceaccountid") //
                .where(restriction) //
                .orderBy(BusinessEntity.Account, "accountid");
        Query query = queryBuilder.build();

        PageFilter pageFilter = new PageFilter(offset, Math.min(QueryTranslator.MAX_ROWS, pageSize));
        query.setPageFilter(pageFilter);

        return query;
    }
}

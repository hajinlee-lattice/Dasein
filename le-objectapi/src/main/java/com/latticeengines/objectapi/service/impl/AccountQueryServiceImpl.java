package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
    public Query generateAccountQuery(String start, Integer offset, Integer pageSize, DataRequest dataRequest) {
        List<Restriction> restrictions = new ArrayList<>();
        if (dataRequest == null) {
            dataRequest = new DataRequest();
        }
        if (StringUtils.isNotBlank(start)) {
            long lastModifiedTime = DateTimeUtils.convertToLongUTCISO8601(start) / 1000;
            Restriction lastModifiedRestriction = Restriction.builder().let(BusinessEntity.Account, "LastModified")
                    .gte(lastModifiedTime).build();
            restrictions.add(lastModifiedRestriction);
        }

        if (CollectionUtils.isNotEmpty(dataRequest.getAccountIds())) {
            RestrictionBuilder accoundIdRestrictionBuilder = Restriction.builder();
            RestrictionBuilder[] accountIdRestrictions = dataRequest.getAccountIds().stream()
                    .map(id -> Restriction.builder().let(BusinessEntity.Account, "AccountId").eq(id))
                    .toArray(RestrictionBuilder[]::new);
            accoundIdRestrictionBuilder.or(accountIdRestrictions);
            restrictions.add(accoundIdRestrictionBuilder.build());
        }

        Restriction restriction = Restriction.builder().and(restrictions).build();

        QueryBuilder queryBuilder = Query.builder();
        if (CollectionUtils.isNotEmpty(dataRequest.getAttributes())) {
            queryBuilder = queryBuilder.select(BusinessEntity.Account,
                    dataRequest.getAttributes().toArray(new String[] {}));
        }
        queryBuilder
                .select(BusinessEntity.Account, "AccountId", "LatticeAccountId", "SalesforceAccountID", "LastModified") //
                .where(restriction) //
                .orderBy(BusinessEntity.Account, "AccountId");
        Query query = queryBuilder.build();

        offset = (offset == null) ? 0 : offset;
        pageSize = (pageSize == null) ? QueryTranslator.MAX_ROWS : pageSize;
        PageFilter pageFilter = new PageFilter(offset, Math.min(QueryTranslator.MAX_ROWS, pageSize));
        query.setPageFilter(pageFilter);

        return query;
    }
}

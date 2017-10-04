package com.latticeengines.playmakercore.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.RestrictionBuilder;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndSort;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;

@Component("pmEntityQueryGenerator")
public class EntityQueryGeneratorImpl implements EntityQueryGenerator {
    private static final int MAX_ROWS = 250;

    @Override
    public FrontEndQuery generateEntityQuery(Long start, DataRequest dataRequest) {
        List<Restriction> restrictions = new ArrayList<>();
        if (dataRequest == null) {
            dataRequest = new DataRequest();
        }
        long lastModifiedTime = start;
        Restriction lastModifiedRestriction = Restriction.builder()
                .let(BusinessEntity.Account, InterfaceName.LastModifiedDate.name()).gte(lastModifiedTime).build();
        restrictions.add(lastModifiedRestriction);

        if (CollectionUtils.isNotEmpty(dataRequest.getAccountIds())) {
            RestrictionBuilder accoundIdRestrictionBuilder = Restriction.builder();
            RestrictionBuilder[] accountIdRestrictions = dataRequest.getAccountIds().stream()
                    .map(id -> Restriction.builder().let(BusinessEntity.Account, InterfaceName.AccountId.name()).eq(id))
                    .toArray(RestrictionBuilder[]::new);
            accoundIdRestrictionBuilder.or(accountIdRestrictions);
            restrictions.add(accoundIdRestrictionBuilder.build());
        }

        Restriction restriction = Restriction.builder().and(restrictions).build();
        FrontEndQuery query = new FrontEndQuery();
        Set<String> attrSet = new HashSet<>();
        attrSet.add(InterfaceName.AccountId.name());
        attrSet.add(InterfaceName.LatticeAccountId.name());
        attrSet.add(InterfaceName.SalesforceAccountID.name());
        attrSet.add(InterfaceName.LastModifiedDate.name());
        if (CollectionUtils.isNotEmpty(dataRequest.getAttributes())) {
            attrSet.addAll(dataRequest.getAttributes());
        }

        query.addLookups(BusinessEntity.Account, attrSet.toArray(new String[attrSet.size()]));

        query.setAccountRestriction(new FrontEndRestriction(restriction));
        List<AttributeLookup> sortFields = new ArrayList<>();
        sortFields.add(new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name()));
        FrontEndSort sort = new FrontEndSort(sortFields, false);
        query.setSort(sort);
        query.setMainEntity(BusinessEntity.Account);
        return query;
    }

    @Override
    public FrontEndQuery generateEntityQuery(Long start, Long offset, Long pageSize, DataRequest dataRequest) {
        FrontEndQuery query = generateEntityQuery(start, dataRequest);

        offset = (offset == null) ? 0 : offset;
        pageSize = (pageSize == null) ? MAX_ROWS : pageSize;
        PageFilter pageFilter = new PageFilter(offset, Math.min(MAX_ROWS, pageSize));
        query.setPageFilter(pageFilter);

        return query;
    }
}

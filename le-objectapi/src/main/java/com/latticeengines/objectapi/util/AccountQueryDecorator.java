package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AccountQueryDecorator extends QueryDecorator {

    private final boolean dataQuery;

    private AccountQueryDecorator(boolean dataQuery) {
        this.dataQuery = dataQuery;
    }

    @Override
    public AttributeLookup getIdLookup() {
        return new AttributeLookup(BusinessEntity.Account, InterfaceName.AccountId.name());
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Account;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] {InterfaceName.CompanyName.name(), InterfaceName.Website.name() };
    }

    @Override
    public boolean isDataQuery() {
        return dataQuery;
    }

    public static final AccountQueryDecorator DATA_QUERY = new AccountQueryDecorator(true);
    public static final AccountQueryDecorator COUNT_QUERY = new AccountQueryDecorator(false);

}

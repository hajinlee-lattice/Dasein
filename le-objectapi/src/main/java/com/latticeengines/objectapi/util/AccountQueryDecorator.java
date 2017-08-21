package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AccountQueryDecorator extends QueryDecorator {

    private final boolean addSelects;

    private AccountQueryDecorator(boolean addSelect) {
        this.addSelects = addSelect;
    }

    @Override
    public BusinessEntity getLookupEntity() {
        return BusinessEntity.Account;
    }

    @Override
    public String[] getEntityLookups() {
        return new String[] { "LDC_Domain", "LDC_Name", "LDC_Country", "LDC_City", "LDC_State", "SalesforceAccountID" };
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Account;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] { "LDC_Domain", "LDC_Name" };
    }

    @Override
    public boolean addSelects() {
        return addSelects;
    }

    public static final AccountQueryDecorator WITH_SELECTS = new AccountQueryDecorator(true);
    public static final AccountQueryDecorator WITHOUT_SELECTS = new AccountQueryDecorator(false);

}

package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class AccountQueryDecorator extends QueryDecorator {

    private final boolean addSelects;

    private final static AttributeLookup[] attributeLookups = {
            new AttributeLookup(BusinessEntity.Account, "LDC_Domain"),
            new AttributeLookup(BusinessEntity.Account, "LDC_Name"),
            new AttributeLookup(BusinessEntity.Account, "LDC_Country"),
            new AttributeLookup(BusinessEntity.Account, "LDC_City"),
            new AttributeLookup(BusinessEntity.Account, "LDC_State"),
            new AttributeLookup(BusinessEntity.Account, "SalesforceAccountID")
    };


    private AccountQueryDecorator(boolean addSelect) {
        this.addSelects = addSelect;
    }

    @Override
    public AttributeLookup[] getAttributeLookups() {
        return attributeLookups;
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

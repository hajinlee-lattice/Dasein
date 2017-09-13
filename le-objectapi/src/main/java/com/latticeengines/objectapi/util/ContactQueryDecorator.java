package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ContactQueryDecorator extends QueryDecorator {

    private final boolean addSelects;

    private final static AttributeLookup[] attributeLookups = {
            new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.toString()),
            new AttributeLookup(BusinessEntity.Contact, InterfaceName.Email.toString()),
            new AttributeLookup(BusinessEntity.Contact, InterfaceName.CRMId.toString()),
            new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.toString()),
            new AttributeLookup(BusinessEntity.Account, InterfaceName.LDC_Name.toString()),
    };

    private ContactQueryDecorator(boolean addSelect) {
        this.addSelects = addSelect;
    }

    @Override
    public AttributeLookup[] getAttributeLookups() {
        return attributeLookups;
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] { InterfaceName.ContactName.toString(), InterfaceName.Email.toString() };
    }

    @Override
    public boolean addSelects() {
        return addSelects;
    }

    public static final ContactQueryDecorator WITH_SELECTS = new ContactQueryDecorator(true);
    public static final ContactQueryDecorator WITHOUT_SELECTS = new ContactQueryDecorator(false);

}

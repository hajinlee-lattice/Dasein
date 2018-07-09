package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ContactQueryDecorator extends QueryDecorator {

    private final boolean dataQuery;

    private ContactQueryDecorator(boolean dataQuery) {
        this.dataQuery = dataQuery;
    }

    @Override
    public AttributeLookup getIdLookup() {
        return new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactId.name());
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    public AttributeLookup[] getFreeTextSearchAttrs() {
        return new AttributeLookup[]{ //
            new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), //
            new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.name()), //
            new AttributeLookup(BusinessEntity.Contact, InterfaceName.Email.name()) //
        };
    }

    @Override
    public boolean isDataQuery() {
        return dataQuery;
    }

    public static final ContactQueryDecorator DATA_QUERY = new ContactQueryDecorator(true);
    public static final ContactQueryDecorator COUNT_QUERY = new ContactQueryDecorator(false);

}

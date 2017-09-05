package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ContactQueryDecorator extends QueryDecorator {

    private final boolean addSelects;

    private ContactQueryDecorator(boolean addSelect) {
        this.addSelects = addSelect;
    }

    @Override
    public BusinessEntity getLookupEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    public String[] getEntityLookups() {
        return new String[] { InterfaceName.ContactName.toString(), InterfaceName.CompanyName.toString(),
                InterfaceName.Email.toString(), InterfaceName.CRMId.toString() };
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Contact;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] { InterfaceName.ContactName.toString(), InterfaceName.CompanyName.toString(),
                InterfaceName.Email.toString() };
    }

    @Override
    public boolean addSelects() {
        return addSelects;
    }

    public static final ContactQueryDecorator WITH_SELECTS = new ContactQueryDecorator(true);
    public static final ContactQueryDecorator WITHOUT_SELECTS = new ContactQueryDecorator(false);

}

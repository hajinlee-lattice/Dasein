package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class EntityMatchAccountQueryDecorator extends AccountQueryDecorator {

    private EntityMatchAccountQueryDecorator(boolean dataQuery) {
        super(dataQuery);
    }

    @Override
    public AttributeLookup[] getFreeTextSearchAttrs() {
        return new AttributeLookup[] { //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.CompanyName.name()), //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.City.name()), //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.State.name()), //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.Country.name()), //
                new AttributeLookup(BusinessEntity.Account, InterfaceName.CustomerAccountId.name()) //
        };
    }

    public static final EntityMatchAccountQueryDecorator DATA_QUERY = new EntityMatchAccountQueryDecorator(true);
    public static final EntityMatchAccountQueryDecorator COUNT_QUERY = new EntityMatchAccountQueryDecorator(false);

}

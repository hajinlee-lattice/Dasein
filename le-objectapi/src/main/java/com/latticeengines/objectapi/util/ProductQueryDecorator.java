package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProductQueryDecorator extends QueryDecorator {

    private final boolean addSelects;

    private final static AttributeLookup[] attributeLookups = {
            new AttributeLookup(BusinessEntity.Product, InterfaceName.ProductId.toString()),
            new AttributeLookup(BusinessEntity.Product, InterfaceName.ProductName.toString())
    };

    private ProductQueryDecorator(boolean addSelect) {
        this.addSelects = addSelect;
    }

    @Override
    public AttributeLookup[] getAttributeLookups() {
        return attributeLookups;
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Product;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] { InterfaceName.ProductId.toString(), InterfaceName.ProductId.toString() };
    }

    @Override
    public boolean addSelects() {
        return addSelects;
    }

    public static final ProductQueryDecorator WITH_SELECTS = new ProductQueryDecorator(true);
    public static final ProductQueryDecorator WITHOUT_SELECTS = new ProductQueryDecorator(false);

}

package com.latticeengines.objectapi.util;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProductQueryDecorator extends QueryDecorator {

    private final boolean dataQuery;

    private ProductQueryDecorator(boolean dataQuery) {
        this.dataQuery = dataQuery;
    }

    @Override
    public AttributeLookup getIdLookup() {
        return new AttributeLookup(BusinessEntity.Product, InterfaceName.ProductId.name());
    }

    @Override
    public BusinessEntity getFreeTextSearchEntity() {
        return BusinessEntity.Product;
    }

    @Override
    public String[] getFreeTextSearchAttrs() {
        return new String[] { InterfaceName.ProductId.toString(), InterfaceName.ProductName.toString() };
    }

    @Override
    public boolean isDataQuery() {
        return dataQuery;
    }

    public static final ProductQueryDecorator DATA_QUERY = new ProductQueryDecorator(true);
    public static final ProductQueryDecorator COUNT_QUERY = new ProductQueryDecorator(false);

}

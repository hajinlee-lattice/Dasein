package com.latticeengines.objectapi.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.objectapi.util.AccountQueryDecorator;
import com.latticeengines.objectapi.util.ContactQueryDecorator;
import com.latticeengines.objectapi.util.ProductQueryDecorator;
import com.latticeengines.objectapi.util.QueryDecorator;

public abstract class BaseQueryServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(BaseQueryServiceImpl.class);

    QueryDecorator getDecorator(BusinessEntity entity, boolean isDataQuery) {
        switch (entity) {
        case Account:
            return isDataQuery ? AccountQueryDecorator.DATA_QUERY : AccountQueryDecorator.COUNT_QUERY;
        case Contact:
            return isDataQuery ? ContactQueryDecorator.DATA_QUERY : ContactQueryDecorator.COUNT_QUERY;
        case Product:
            return isDataQuery ? ProductQueryDecorator.DATA_QUERY : ProductQueryDecorator.COUNT_QUERY;
        default:
            log.warn("Cannot find a decorator for entity " + entity);
            return null;
        }
    }

}

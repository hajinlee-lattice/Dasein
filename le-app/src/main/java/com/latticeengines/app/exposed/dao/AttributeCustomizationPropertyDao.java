package com.latticeengines.app.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationPropertyDao extends BaseDao<AttributeCustomizationProperty> {
    AttributeCustomizationProperty find(String name, AttributeUseCase useCase, String propertyName);
}

package com.latticeengines.app.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationPropertyEntityMgr extends BaseEntityMgr<AttributeCustomizationProperty> {
    AttributeCustomizationProperty find(String attributeName, AttributeUseCase useCase, String propertyName);

    List<AttributeCustomizationProperty> find(String attributeName);
}

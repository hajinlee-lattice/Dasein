package com.latticeengines.app.exposed.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationEntityMgr extends BaseEntityMgr<AttributeCustomization> {
    AttributeCustomization find(String attributeName, AttributeUseCase useCase);

    List<AttributeCustomization> find(String attributeName);
}

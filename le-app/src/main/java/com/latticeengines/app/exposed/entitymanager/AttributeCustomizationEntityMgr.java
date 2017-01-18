package com.latticeengines.app.exposed.entitymanager;

import com.latticeengines.datafabric.entitymanager.CompositeFabricEntityMgr;
import com.latticeengines.domain.exposed.attribute.AttributeCustomization;

public interface AttributeCustomizationEntityMgr extends CompositeFabricEntityMgr<AttributeCustomization> {
    AttributeCustomization find(String attributeName);
}

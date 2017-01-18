package com.latticeengines.app.exposed.entitymanager.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.datafabric.entitymanager.impl.CompositeFabricEntityMgrImpl;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.attribute.AttributeCustomization;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("attributeCustomizationEntityMgr")
public class AttributeCustomizationEntityMgrImpl extends CompositeFabricEntityMgrImpl<AttributeCustomization> implements
        AttributeCustomizationEntityMgr {

    @Autowired
    public AttributeCustomizationEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService) {
        super(new BaseFabricEntityMgrImpl.Builder().messageService(messageService).dataService(dataService) //
                .recordType("AttributeCustomization").topic("AttributeCustomization") //
                .scope(TopicScope.ENVIRONMENT_PRIVATE).store("DYNAMO").repository("Ulysses"));
    }

    @Override
    public AttributeCustomization find(String name) {
        Map<String, String> properties = new HashMap<>();
        properties.put("parentKey", MultiTenantContext.getCustomerSpace().toString() + "_AttributeCustomization");
        properties.put("entityId", name);

        List<AttributeCustomization> customizations = findByProperties(properties);
        if (customizations.size() > 0) {
            return customizations.get(0);
        }
        return null;
    }

}

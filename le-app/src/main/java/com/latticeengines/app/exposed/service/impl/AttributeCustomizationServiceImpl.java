package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("attributeCustomizationService")
public class AttributeCustomizationServiceImpl implements AttributeCustomizationService {
    private static final Logger log = Logger.getLogger(AttributeCustomizationServiceImpl.class);

    @Autowired
    private AttributeCustomizationEntityMgr attributeCustomizationEntityMgr;

    @Override
    public void save(String name, AttributeUseCase useCase, AttributeFlags flags) {
        log.info(String.format("Customizing attribute %s for tenant %s and use case %s with flags %s", name,
                MultiTenantContext.getCustomerSpace(), useCase, flags));
        AttributeCustomization customization = new AttributeCustomization();
        customization.setName(name);
        customization.setUseCase(useCase);
        customization.setFlags(flags);
        attributeCustomizationEntityMgr.createOrUpdate(customization);
    }

    @Override
    public AttributeFlags retrieve(String name, AttributeUseCase useCase) {
        return attributeCustomizationEntityMgr.find(name, useCase).getFlags();
    }

    @Override
    public void addFlags(List<LeadEnrichmentAttribute> attributes) {
        List<AttributeCustomization> allCustomizations = attributeCustomizationEntityMgr.findAll();
        Map<String, List<AttributeCustomization>> customizationMap = new HashMap<>();
        for (AttributeCustomization customization : allCustomizations) {
            List<AttributeCustomization> list = customizationMap.get(customization.getName());
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(customization);
            customizationMap.put(customization.getName(), list);
        }

        for (LeadEnrichmentAttribute attribute : attributes) {
            List<AttributeCustomization> customizations = customizationMap.get(attribute.getFieldName());
            if (customizations != null) {
                Map<AttributeUseCase, AttributeFlags> flagsMap = new HashMap<>();
                for (AttributeCustomization customization : customizations) {
                    flagsMap.put(customization.getUseCase(), customization.getFlags());
                }
                attribute.setAttributeFlagsMap(flagsMap);
            }
        }
    }
}

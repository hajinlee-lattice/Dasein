package com.latticeengines.app.exposed.service.impl;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
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
}

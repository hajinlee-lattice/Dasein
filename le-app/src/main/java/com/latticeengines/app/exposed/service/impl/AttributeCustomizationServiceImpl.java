package com.latticeengines.app.exposed.service.impl;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.domain.exposed.attribute.AttributeCustomization;
import com.latticeengines.domain.exposed.attribute.AttributeFlags;
import com.latticeengines.domain.exposed.attribute.AttributeUseCase;
import com.latticeengines.domain.exposed.attribute.CompanyProfileAttributeFlags;
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
        AttributeCustomization customization = retrieve(name);
        if (customization == null) {
            customization = new AttributeCustomization();
            customization.setTenant(MultiTenantContext.getTenant());
            customization.setName(name);
        }
        switch (useCase) {
        case CompanyProfile:
            customization.setCompanyProfileFlags((CompanyProfileAttributeFlags) flags);
            break;
        default:
            throw new RuntimeException(String.format("Invalid use case specified %s", useCase));
        }

        // createOrUpdate
        attributeCustomizationEntityMgr.create(customization);
    }

    @Override
    public AttributeFlags getFlags(String name, AttributeUseCase useCase) {
        AttributeCustomization customization = retrieve(name);
        switch (useCase) {
        case CompanyProfile:
            return customization.getCompanyProfileFlags();
        default:
            throw new RuntimeException(String.format("Invalid use case specified %s", useCase));
        }
    }

    @Override
    public AttributeCustomization retrieve(String name) {
        return attributeCustomizationEntityMgr.find(name);
    }
}

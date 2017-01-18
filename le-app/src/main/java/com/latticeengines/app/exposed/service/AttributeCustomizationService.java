package com.latticeengines.app.exposed.service;

import com.latticeengines.domain.exposed.attribute.AttributeCustomization;
import com.latticeengines.domain.exposed.attribute.AttributeFlags;
import com.latticeengines.domain.exposed.attribute.AttributeUseCase;

public interface AttributeCustomizationService {
    void save(String name, AttributeUseCase useCase, AttributeFlags flags);

    AttributeFlags getFlags(String name, AttributeUseCase useCase);

    AttributeCustomization retrieve(String name);
}

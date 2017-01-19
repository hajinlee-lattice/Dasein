package com.latticeengines.app.exposed.service;

import com.latticeengines.domain.exposed.pls.AttributeFlags;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationService {
    void save(String name, AttributeUseCase useCase, AttributeFlags flags);

    AttributeFlags retrieve(String name, AttributeUseCase useCase);
}

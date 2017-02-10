package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

public interface AttributeCustomizationService {
    void save(String name, AttributeUseCase useCase, String propertyName, String value);

    String retrieve(String name, AttributeUseCase useCase, String propertyName);

    void addFlags(List<LeadEnrichmentAttribute> attributes);

    void saveCategory(Category category, AttributeUseCase useCase, String propertyName, String value);

    void saveSubCategory(Category category, String subcategoryName, AttributeUseCase useCase,
            String propertyName, String value);
}

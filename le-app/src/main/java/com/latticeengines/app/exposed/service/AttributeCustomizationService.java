package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;

public interface AttributeCustomizationService {
    void save(String name, AttributeUseCase useCase, String propertyName, String value);

    void save(String name, AttributeUseCase useCase, Map<String, String> properties);

    String retrieve(String name, AttributeUseCase useCase, String propertyName);

    void addFlags(List<HasAttributeCustomizations> attributes);

    void saveCategory(Category category, AttributeUseCase useCase, String propertyName, String value);

    void saveSubcategory(Category category, String subcategoryName, AttributeUseCase useCase, String propertyName,
            String value);

    void saveCategory(Category category, AttributeUseCase useCase, Map<String, String> properties);

    void saveSubcategory(Category category, String subcategoryName, AttributeUseCase useCase,
            Map<String, String> properties);
}

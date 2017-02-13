package com.latticeengines.app.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationPropertyDao extends BaseDao<AttributeCustomizationProperty> {
    AttributeCustomizationProperty find(String name, AttributeUseCase useCase, String propertyName);

    void deleteSubcategory(String categoryName, AttributeUseCase useCase, String propertyName);

    void deleteCategory(Category category, AttributeUseCase useCase, String propertyName);
}

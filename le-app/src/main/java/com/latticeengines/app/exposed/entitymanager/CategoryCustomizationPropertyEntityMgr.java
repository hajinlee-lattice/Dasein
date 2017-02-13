package com.latticeengines.app.exposed.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;

public interface CategoryCustomizationPropertyEntityMgr extends BaseEntityMgr<CategoryCustomizationProperty>{

    CategoryCustomizationProperty find(AttributeUseCase useCase, String categoryName, String propertyName);

    void deleteSubcategories(Category category, AttributeUseCase useCase, String propertyName);

}

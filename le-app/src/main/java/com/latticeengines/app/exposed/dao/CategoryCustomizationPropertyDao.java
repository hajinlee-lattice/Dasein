package com.latticeengines.app.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;

public interface CategoryCustomizationPropertyDao extends BaseDao<CategoryCustomizationProperty> {

    CategoryCustomizationProperty find(AttributeUseCase useCase, String categoryName, String propertyName);

}

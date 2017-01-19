package com.latticeengines.app.exposed.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.AttributeCustomization;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;

public interface AttributeCustomizationDao extends BaseDao<AttributeCustomization> {
    AttributeCustomization find(String name, AttributeUseCase useCase);
}

package com.latticeengines.apps.core.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("attrConfigServiceTestImpl")
public class AttrConfigServiceTestImpl extends AbstractAttrConfigService {

    @Override
    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        return null;
    }

    @Override
    protected List<ColumnMetadata> getSystemMetadata(Category category) {
        return null;
    }

}

package com.latticeengines.apps.dcp.service.impl;

import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * DCP does not need an attr config service, the tenant will use the cdl one.
 * So here is just a dummy bean
 */
@Service("dcpAttrConfigService")
public class DCPAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Override
    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        throw new UnsupportedOperationException("Not supported!");
    }

    @Override
    protected List<ColumnMetadata> getSystemMetadata(Category category) {
        throw new UnsupportedOperationException("Not supported!");
    }

}

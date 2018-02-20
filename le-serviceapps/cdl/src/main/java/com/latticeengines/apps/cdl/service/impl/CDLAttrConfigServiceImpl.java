package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        return Collections.emptyList();
    }

}

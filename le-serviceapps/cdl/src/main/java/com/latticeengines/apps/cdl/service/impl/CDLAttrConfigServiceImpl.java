package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getServingStore());
        if (StringUtils.isBlank(tableName)) {
            return Collections.emptyList();
        } else {
            return metadataProxy.getTableColumns(customerSpace, tableName);
        }
    }

}

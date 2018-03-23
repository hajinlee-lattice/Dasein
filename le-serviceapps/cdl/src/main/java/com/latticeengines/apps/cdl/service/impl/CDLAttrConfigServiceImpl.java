package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getTenantId();
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(tenantId);
        return servingStoreService.getSystemMetadata(entity, version) //
                .sequential().collectList().block();
    }

    @Override
    public List<AttrConfig> getRenderedList(String tenantId, BusinessEntity entity) {
        if (entity != null) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(entity);
            return render(columns, customConfig);
        } else {
            List<AttrConfig> renderedList = new ArrayList<>();
            BusinessEntity.SEGMENT_ENTITIES.forEach(e -> {
                List<AttrConfig> list = getRenderedList(tenantId, e);
                if (list != null && list.size() != 0) {
                    renderedList.addAll(list);
                }
            });
            return renderedList;
        }
    }

}

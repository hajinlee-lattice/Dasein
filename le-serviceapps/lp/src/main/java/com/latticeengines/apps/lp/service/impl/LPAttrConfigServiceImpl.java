package com.latticeengines.apps.lp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;


@Service("lpAttrConfigService")
public class LPAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private AMMetadataStore amMetadataStore;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            return amMetadataStore.getMetadata("").collectList().block();
        } else {
            throw new UnsupportedOperationException("Entity " + entity + " is not supported.");
        }
    }

    @Override
    public List<AttrConfig> getRenderedList(String tenantId, BusinessEntity entity) {
        List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
        List<ColumnMetadata> columns = getSystemMetadata(entity);
        List<AttrConfig> renderedList = render(columns, customConfig);
        return renderedList;
    }
}

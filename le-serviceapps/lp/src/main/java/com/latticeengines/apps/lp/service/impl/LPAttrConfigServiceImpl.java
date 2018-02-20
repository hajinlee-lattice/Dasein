package com.latticeengines.apps.lp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;


@Service("lpAttrConfigService")
public class LPAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        if (BusinessEntity.LatticeAccount.equals(entity)) {
            return columnMetadataProxy.getAllColumns();
        } else {
            throw new UnsupportedOperationException("Entity " + entity + " is not supported.");
        }
    }

}

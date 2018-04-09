package com.latticeengines.apps.lp.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.core.mds.AMMetadataStore;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.CategoryUtils;


@Service("lpAttrConfigService")
public class LPAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private AMMetadataStore amMetadataStore;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        if (BusinessEntity.Account.equals(entity)) {
            return amMetadataStore.getMetadata("") //
                    .map(cm -> {
                        cm.setEntity(BusinessEntity.Account);
                        return cm;
                    }).collectList().block();
        } else {
            throw new UnsupportedOperationException("Entity " + entity + " is not supported.");
        }
    }

    protected List<ColumnMetadata> getSystemMetadata(Category category) {
        BusinessEntity entity = CategoryUtils.getEntity(category);
        if (BusinessEntity.Account.equals(entity)) {
            return amMetadataStore.getMetadata("") //
                    .filter(cm -> category.equals(cm.getCategory())) //
                    .map(cm -> {
                        cm.setEntity(BusinessEntity.Account);
                        return cm;
                    })
                    .collectList().block();
        } else {
            throw new UnsupportedOperationException("Entity " + entity + " is not supported.");
        }
    }

}

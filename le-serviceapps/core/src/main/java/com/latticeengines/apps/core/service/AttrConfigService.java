package com.latticeengines.apps.core.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render);

    List<AttrConfig> getRenderedList(Category category);

    List<AttrConfig> getRenderedList(Category category, String attributeSetName);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean onlyActive);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories, List<String> propertyNames,
                                                                     boolean onlyActive, String attributeSetName);

    AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode);

    AttrConfigRequest validateRequest(AttrConfigRequest request, AttrConfigUpdateMode mode);

    Map<BusinessEntity, List<AttrConfig>> findAllHaveCustomDisplayNameByTenantId(String tenantId);

    void removeAttrConfig(String tenantId);

    void removeAttrConfigForEntity(String tenantId, BusinessEntity entity);

    List<AttrConfig> getRenderedList(String propertyName, Boolean enabled);

    AttributeSet getAttributeSetByName(String name);

    List<AttributeSet> getAttributeSets(boolean withAttributesMap);

    AttributeSet cloneAttributeSet(String name, AttributeSet attributeSet);

    AttributeSet updateAttributeSet(AttributeSet attributeSet);

    AttributeSet createAttributeSet(AttributeSet attributeSet);

    void deleteAttributeSetByName(String name);

    AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode, boolean updateDefaultSet);
}

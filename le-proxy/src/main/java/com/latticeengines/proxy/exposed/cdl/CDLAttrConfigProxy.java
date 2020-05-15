package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Map;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;

public interface CDLAttrConfigProxy {

    AttrConfigRequest getAttrConfigByEntity(String customerSpace, BusinessEntity entity, boolean render);

    AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName);

    AttrConfigRequest getAttrConfigByCategory(String customerSpace, String categoryName, String attributeSetName);

    AttrConfigRequest getAttrConfigByProperty(String customerSpace, String propertyName, boolean enabled);

    AttrConfigRequest saveAttrConfig(String customerSpace, AttrConfigRequest request, AttrConfigUpdateMode mode);

    AttrConfigRequest validateAttrConfig(String customerSpace, AttrConfigRequest request, AttrConfigUpdateMode mode);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace,
                                                                     @Nullable List<String> categoryNames, @NonNull List<String> propertyNames, boolean activeOnly);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(String customerSpace, @Nullable List<String> categoryNames,
                                                                     @NonNull List<String> propertyNames, boolean activeOnly, String attributeSetName);

    Map<BusinessEntity, List<AttrConfig>> getCustomDisplayNames(String customerSpace);

    void removeAttrConfigByTenantAndEntity(String customerSpace, BusinessEntity entity);

    AttributeSet getAttributeSet(String customerSpace, String name);

    List<AttributeSet> getAttributeSets(String customerSpace);

    AttributeSet cloneAttributeSet(String customerSpace, String attributeSetName, AttributeSet attributeSet);

    AttributeSet updateAttributeSet(String customerSpace, AttributeSet attributeSet);

    void deleteAttributeSet(String customerSpace, String name);

}

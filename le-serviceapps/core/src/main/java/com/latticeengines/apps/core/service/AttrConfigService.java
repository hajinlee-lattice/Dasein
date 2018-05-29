package com.latticeengines.apps.core.service;

import java.util.List;
import java.util.Map;

import org.springframework.lang.Nullable;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render);

    List<AttrConfig> getRenderedList(Category category);

    List<AttrConfigOverview<?>> getAttrConfigOverview(@Nullable Category category, String propertyName);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean onlyActive);

    AttrConfigRequest validateRequest(AttrConfigRequest request, boolean isAdmin);

    AttrConfigRequest saveRequest(AttrConfigRequest request, boolean isAdmin);

}

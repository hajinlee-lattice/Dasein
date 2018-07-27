package com.latticeengines.apps.core.service;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.RequestParam;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigCategoryOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render);

    List<AttrConfig> getRenderedList(Category category);

    Map<String, AttrConfigCategoryOverview<?>> getAttrConfigOverview(List<Category> categories,
            List<String> propertyNames, boolean onlyActive);

    AttrConfigRequest saveRequest(AttrConfigRequest request, AttrConfigUpdateMode mode);

    AttrConfigRequest validateRequest(AttrConfigRequest request, AttrConfigUpdateMode mode);

}

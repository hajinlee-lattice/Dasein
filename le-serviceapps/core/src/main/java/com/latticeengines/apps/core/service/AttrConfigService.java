package com.latticeengines.apps.core.service;

import java.util.List;

import org.springframework.lang.Nullable;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigOverview;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render);

    List<AttrConfig> getRenderedList(Category category);

    List<AttrConfigOverview<?>> getAttrConfigOverview(@Nullable Category category, String propertyName);

    AttrConfigRequest validateRequest(AttrConfigRequest request);

    AttrConfigRequest saveRequest(AttrConfigRequest request);

}

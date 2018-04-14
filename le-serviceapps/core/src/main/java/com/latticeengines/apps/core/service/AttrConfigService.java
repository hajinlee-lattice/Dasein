package com.latticeengines.apps.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render);

    List<AttrConfig> getRenderedList(Category category);

    AttrConfigRequest validateRequest(AttrConfigRequest request);

    AttrConfigRequest saveRequest(AttrConfigRequest request);

}

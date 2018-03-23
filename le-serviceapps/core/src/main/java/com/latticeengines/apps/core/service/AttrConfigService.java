package com.latticeengines.apps.core.service;

import java.util.List;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;

public interface AttrConfigService {

    List<AttrConfig> getRenderedList(String customerSpace, BusinessEntity entity);
}

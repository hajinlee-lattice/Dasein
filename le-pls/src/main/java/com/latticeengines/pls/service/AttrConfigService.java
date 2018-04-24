package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;

public interface AttrConfigService {

    AttrConfigActivationOverview getAttrConfigActivationOverview(Category category);

    AttrConfigUsageOverview getAttrConfigUsageOverview();

    void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    void updateUsageConfig(String categoryName, String usage, AttrConfigSelectionRequest request);
}

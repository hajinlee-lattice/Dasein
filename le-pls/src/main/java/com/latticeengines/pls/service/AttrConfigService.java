package com.latticeengines.pls.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;

public interface AttrConfigService {

    AttrConfigActivationOverview getAttrConfigActivationOverview(Category category);

    Map<String, AttrConfigActivationOverview> getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getAttrConfigUsageOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usage);

    void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    void updateUsageConfig(String categoryName, String usage, AttrConfigSelectionRequest request);
}

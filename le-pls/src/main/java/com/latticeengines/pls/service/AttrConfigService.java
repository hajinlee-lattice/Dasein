package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;

public interface AttrConfigService {

    List<AttrConfigActivationOverview> getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryDisplayName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryDisplayName, String usageName);

    void updateActivationConfig(String categoryDisplayName, AttrConfigSelectionRequest request);

    void updateUsageConfig(String categoryDisplayName, String usageName, AttrConfigSelectionRequest request);
}

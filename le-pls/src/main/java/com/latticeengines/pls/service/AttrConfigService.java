package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigActivationOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.pls.service.impl.AttrConfigServiceImpl.UpdateUsageResponse;

public interface AttrConfigService {

    List<AttrConfigActivationOverview> getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryDisplayName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryDisplayName, String usageName);

    void updateActivationConfig(String categoryDisplayName, AttrConfigSelectionRequest request);

    UpdateUsageResponse updateUsageConfig(String categoryDisplayName, String usageName,
            AttrConfigSelectionRequest request);

    Map<String, AttributeStats> getStats(String catDisplayName, @PathVariable String subcatName);
}

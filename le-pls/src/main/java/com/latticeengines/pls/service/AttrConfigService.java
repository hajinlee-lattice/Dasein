package com.latticeengines.pls.service;

import java.util.Map;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.pls.service.impl.AttrConfigServiceImpl.UpdateUsageResponse;

public interface AttrConfigService {

    AttrConfigStateOverview getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usageName);

    void updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    UpdateUsageResponse updateUsageConfig(String categoryName, String usageName, AttrConfigSelectionRequest request);

    Map<String, AttributeStats> getStats(String categoryName, @PathVariable String subcatName);
}

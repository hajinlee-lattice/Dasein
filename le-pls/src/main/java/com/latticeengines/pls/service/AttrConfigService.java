package com.latticeengines.pls.service;

import java.util.Map;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigNameAndDescription;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public interface AttrConfigService {

    AttrConfigStateOverview getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigStateOverview getOverallAttrConfigNameOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetails(String categoryName, String usageName);

    UIAction updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    UIAction updateUsageConfig(String categoryName, String usageName, AttrConfigSelectionRequest request);

    void updateNameConfig(String categoryName, Map<String, AttrConfigNameAndDescription> request);

    Map<String, AttributeStats> getStats(String categoryName, @PathVariable String subcatName);
}

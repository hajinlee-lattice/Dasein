package com.latticeengines.pls.service;

import java.util.Map;

import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;

public interface AttrConfigService {

    AttrConfigStateOverview getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigStateOverview getOverallAttrConfigNameOverview();

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForUsage(String categoryName, String usageName);

    SubcategoryDetail getAttrConfigSelectionDetailForName(String categoryName);

    UIAction updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    UIAction updateUsageConfig(String categoryName, String usageName, AttrConfigSelectionRequest request);

    SubcategoryDetail updateNameConfig(String categoryName, SubcategoryDetail request);

    Map<String, AttributeStats> getStats(String categoryName, @PathVariable String subcatName);
}

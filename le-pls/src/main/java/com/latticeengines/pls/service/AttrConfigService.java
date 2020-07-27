package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.exception.UIAction;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionDetail.SubcategoryDetail;
import com.latticeengines.domain.exposed.pls.AttrConfigSelectionRequest;
import com.latticeengines.domain.exposed.pls.AttrConfigStateOverview;
import com.latticeengines.domain.exposed.pls.AttrConfigUsageOverview;

public interface AttrConfigService {

    AttrConfigStateOverview getOverallAttrConfigActivationOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview();

    AttrConfigStateOverview getOverallAttrConfigNameOverview();

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview(String attributeSetName);

    AttrConfigUsageOverview getOverallAttrConfigUsageOverview(String attributeSetName, List<String> usagePropertyList);

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForState(String categoryName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForUsage(String categoryName, String usageName);

    AttrConfigSelectionDetail getAttrConfigSelectionDetailForUsage(String categoryName, String usageName, String attributeSetName);

    SubcategoryDetail getAttrConfigSelectionDetailForName(String categoryName);

    UIAction updateActivationConfig(String categoryName, AttrConfigSelectionRequest request);

    UIAction updateUsageConfig(String categoryName, String usageName, AttrConfigSelectionRequest request);

    SubcategoryDetail updateNameConfig(String categoryName, SubcategoryDetail request);

    Map<String, AttributeStats> getStats(String categoryName, String subcatName);

    AttributeSet getAttributeSet(String name);

    List<AttributeSet> getAttributeSets();

    AttributeSet cloneAttributeSet(String attributeSetName, AttributeSet attributeSet);

    AttributeSet updateAttributeSet(AttributeSet attributeSet);

    AttributeSet createAttributeSet(AttributeSet attributeSet);

    boolean deleteAttributeSet(String name);
}

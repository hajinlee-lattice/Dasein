package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;

public interface TalkingPointAttributeService {
    List<TalkingPointAttribute> getAccountAttributes(String customerSpace);

    List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace);

    List<TalkingPointAttribute> getVariableAttributes(String customerSpace);

    TalkingPointNotionAttributes getAttributesForNotions(List<String> notions, String customerSpace);
}
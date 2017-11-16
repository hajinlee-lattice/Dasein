package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;

public interface TalkingPointAttributeService {
    List<TalkingPointAttribute> getAccountAttributes(String customerSpace);

    List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace);

    List<TalkingPointAttribute> getVariableAttributes(String customerSpace);

    TalkingPointNotionAttributes getAttributesForNotions(List<String> notions, String customerSpace);
}
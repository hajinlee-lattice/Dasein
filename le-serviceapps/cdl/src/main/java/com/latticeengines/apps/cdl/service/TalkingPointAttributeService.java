package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.TalkingPointAttribute;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;

public interface TalkingPointAttributeService {
    List<TalkingPointAttribute> getAccountAttributes();

    List<TalkingPointAttribute> getRecommendationAttributes();

    List<TalkingPointAttribute> getVariableAttributes();

    TalkingPointNotionAttributes getAttributesForNotions(List<String> notions);
}

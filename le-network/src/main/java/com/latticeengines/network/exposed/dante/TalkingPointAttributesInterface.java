package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
import com.latticeengines.domain.exposed.dante.TalkingPointNotionAttributes;

public interface TalkingPointAttributesInterface {
    List<TalkingPointAttribute> getAccountAttributes(String customerSpace);

    List<TalkingPointAttribute> getRecommendationAttributes(String customerSpace);

    TalkingPointNotionAttributes getAttributesByNotions(List<String> notions, String customerSpace);
}

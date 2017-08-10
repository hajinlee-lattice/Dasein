package com.latticeengines.network.exposed.dante;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;

public interface DanteAttributesInterface {
    List<DanteAttribute> getAccountAttributes(String customerSpace);

    List<DanteAttribute> getRecommendationAttributes(String customerSpace);

    DanteNotionAttributes getAttributesByNotions(List<String> notions, String customerSpace);
}

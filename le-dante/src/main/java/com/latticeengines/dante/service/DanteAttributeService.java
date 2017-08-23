package com.latticeengines.dante.service;

import java.util.List;

import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DanteNotionAttributes;

public interface DanteAttributeService {
    List<DanteAttribute> getAccountAttributes(String customerSpace);

    List<DanteAttribute> getRecommendationAttributes(String customerSpace);

    List<DanteAttribute> getVariableAttributes(String customerSpace);

    DanteNotionAttributes getAttributesForNotions(List<String> notions, String customerSpace);
}
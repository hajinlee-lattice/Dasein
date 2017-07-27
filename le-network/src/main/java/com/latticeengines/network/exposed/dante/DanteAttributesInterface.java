package com.latticeengines.network.exposed.dante;

import java.util.Map;

public interface DanteAttributesInterface {
    Map<String, String> getAccountAttributes(String customerSpace);

    Map<String, String> getRecommendationAttributes(String customerSpace);
}

package com.latticeengines.network.exposed.dante;

import java.util.Map;

import com.latticeengines.domain.exposed.ResponseDocument;

public interface DanteAttributesInterface {
    ResponseDocument<Map<String, String>> getAccountAttributes(String customerSpace);

    ResponseDocument<Map<String, String>> getRecommendationAttributes(String customerSpace);
}

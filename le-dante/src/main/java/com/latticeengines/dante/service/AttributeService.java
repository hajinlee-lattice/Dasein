package com.latticeengines.dante.service;

import java.util.Map;

public interface AttributeService {
    Map<String, String> getAccountAttributes(String customerSpace);

    Map<String, String> getRecommendationAttributes(String customerSpace);
}
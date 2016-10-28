package com.latticeengines.datacloud.match.service;

import java.util.List;
import java.util.Map;

public interface FuzzyMatchService {
    Object callMatch(Map<String, Object> matchRequest) throws Exception;

    List<Object> callMatch(List<Map<String, Object>> matchRequests) throws Exception;

}

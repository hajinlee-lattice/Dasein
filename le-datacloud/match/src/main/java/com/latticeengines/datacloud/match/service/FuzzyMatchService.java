package com.latticeengines.datacloud.match.service;

import java.util.List;

public interface FuzzyMatchService {

    Object callMatch(Object matchRequest) throws Exception;

    List<Object> callMatch(List<Object> matchRequests) throws Exception;

}

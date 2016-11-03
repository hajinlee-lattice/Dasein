package com.latticeengines.sampleapi.sample.service;

import java.util.List;

public interface SampleFuzzyMatchService {
    void callMatch(SampleOutputRecord matchRequest, String dataCloudVersion) throws Exception;

    void callMatch(List<SampleOutputRecord> matchRequests, String dataCloudVersion) throws Exception;

}

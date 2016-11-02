package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public interface FuzzyMatchService {
    void callMatch(OutputRecord matchRequest, String dataCloudVersion) throws Exception;

    void callMatch(List<OutputRecord> matchRequests, String dataCloudVersion) throws Exception;

}

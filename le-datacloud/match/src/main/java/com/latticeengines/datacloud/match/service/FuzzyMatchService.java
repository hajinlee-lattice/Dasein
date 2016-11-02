package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public interface FuzzyMatchService {

    void callMatch(List<OutputRecord> matchRequests, String rootOperationUid, String dataCloudVersion) throws Exception;

}

package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.log4j.Level;

import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public interface FuzzyMatchService {

    <T extends OutputRecord> void callMatch(List<T> matchRecords, String rootOperationUid, String dataCloudVersion,
            String decisionGraph, Level logLevel, boolean useDnBCache) throws Exception;

}

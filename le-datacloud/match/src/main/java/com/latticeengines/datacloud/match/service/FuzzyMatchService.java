package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.log4j.Level;

import scala.concurrent.Future;

import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

public interface FuzzyMatchService {

    <T extends OutputRecord> void callMatch(List<T> matchRecords, String rootOperationUid, String dataCloudVersion,
            String decisionGraph, Level logLevel, boolean useDnBCache) throws Exception;

    <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, String rootOperationUid,
            String dataCloudVersion, String decisionGraph, Level logLevel, boolean useDnBCache) throws Exception;

    <T extends OutputRecord> void fetchIdResult(List<T> matchRecords, Level logLevel, List<Future<Object>> matchFutures)
            throws Exception;
}

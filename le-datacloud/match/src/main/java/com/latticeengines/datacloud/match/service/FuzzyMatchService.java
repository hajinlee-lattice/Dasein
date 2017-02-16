package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.log4j.Level;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

import scala.concurrent.Future;

public interface FuzzyMatchService {

    <T extends OutputRecord> void callMatch(List<T> matchRecords, MatchInput matchInput) throws Exception;

    <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, MatchInput matchInput)
            throws Exception;

    <T extends OutputRecord> void fetchIdResult(List<T> matchRecords, Level logLevel, List<Future<Object>> matchFutures)
            throws Exception;
}

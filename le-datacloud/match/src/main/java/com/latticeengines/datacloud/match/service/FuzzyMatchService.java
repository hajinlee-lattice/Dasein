package com.latticeengines.datacloud.match.service;

import java.util.List;

import org.apache.log4j.Level;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

import scala.concurrent.Future;

public interface FuzzyMatchService {

    /*
     * Blocking request to perform a match on a list of records
     * Effectively calls the below two methods in succession
     */
    <T extends OutputRecord> void callMatch(List<T> matchRecords, MatchInput matchInput) throws Exception;

    /*
     * Generates a set of async match requests
     * @param matchRecords - the records to perform the matches on
     * @param matchInput - match input for the supplied records
     * @return - a list of futures that yield the InternalOutputRecord
     */
    <T extends OutputRecord> List<Future<Object>> callMatchAsync(List<T> matchRecords, MatchInput matchInput)
            throws Exception;

    /*
     * Convert the matchFutures into InternalOutputRecords
     */
    <T extends OutputRecord> void fetchIdResult(List<T> matchRecords, Level logLevel, List<Future<Object>> matchFutures)
            throws Exception;
}

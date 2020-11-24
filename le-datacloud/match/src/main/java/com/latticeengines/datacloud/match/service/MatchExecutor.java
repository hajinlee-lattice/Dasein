package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.datacloud.match.service.impl.MatchContext;

/*
 * Each of the methods here changes MatchContext accordingly and returns the same object as input
 */
public interface MatchExecutor {

    /*
     * Sync method
     *
     * Performs a match for a given MatchContext
     */
    MatchContext execute(MatchContext matchContext);

    /*
     * Sync method
     *
     * Performs matches on a group of MatchContexts
     */
    List<MatchContext> executeBulk(List<MatchContext> matchContexts);

    /*
     * Async/bulk method
     *
     * Generate futures, which are stored in MatchContext
     */
    MatchContext executeAsync(MatchContext matchContext);

    /*
     * Async/bulk method
     *
     * Block until the matches stored in MatchContext are completed
     */
    MatchContext executeMatchResult(MatchContext matchContext);


}

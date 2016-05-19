package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.match.service.MatchFetcher;

@Component("bulkMatchFetcher")
public class BulkMatchFetcher extends MatchFetcherBase implements MatchFetcher {

    @Override
    public MatchContext fetch(MatchContext matchContext) {
        return fetchSync(matchContext);
    }

    @Override
    public List<String> enqueue(List<MatchContext> matchContexts) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

    @Override
    public List<MatchContext> waitForResult(List<String> rootUids) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

}

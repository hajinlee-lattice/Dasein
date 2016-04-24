package com.latticeengines.propdata.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.match.service.MatchFetcher;

@Component("bulkMatchFetcher")
public class BulkMatchFetcher extends MatchFetcherBase implements MatchFetcher {

    @Override
    public MatchContext fetch(MatchContext matchContext) {
        return executeFetch(matchContext);
    }

}

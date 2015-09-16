package com.latticeengines.propdata.api.datasource;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class PropDataMatcherRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return MatcherContextHolder.getMatcherClient();
    }
}
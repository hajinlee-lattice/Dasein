package com.latticeengines.propdata.api.datasource;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

public class MatchClientRoutingDataSource extends AbstractRoutingDataSource {

    @Override
    protected Object determineCurrentLookupKey() {
        return MatchClientContextHolder.getMatchClient();
    }
}
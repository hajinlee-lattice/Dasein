package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;

public interface MatchCommandUpdater {
    
    MatchCommandUpdater status(MatchStatus status);
    MatchCommandUpdater progress(Float progress);
    MatchCommandUpdater errorMessage(String errorMessage);
    MatchCommandUpdater rowsMatched(Integer rowsMatched);
    MatchCommandUpdater resultLocation(String location);
    MatchCommand commit();
    
}

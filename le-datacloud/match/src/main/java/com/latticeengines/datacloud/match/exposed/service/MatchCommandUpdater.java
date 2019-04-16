package com.latticeengines.datacloud.match.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;

public interface MatchCommandUpdater {

    MatchCommandUpdater status(MatchStatus status);
    MatchCommandUpdater progress(Float progress);
    MatchCommandUpdater errorMessage(String errorMessage);
    MatchCommandUpdater rowsRequested(Integer rowsSubmitted);
    MatchCommandUpdater rowsMatched(Integer rowsMatched);
    MatchCommandUpdater matchResults(Map<EntityMatchResult, Long> matchResultMap);
    MatchCommandUpdater dnbCommands();
    MatchCommandUpdater resultLocation(String location);
    MatchCommand commit();

}

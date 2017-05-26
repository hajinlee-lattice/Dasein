package com.latticeengines.datacloud.match.exposed.service;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;

public interface MatchCommandUpdater {

    MatchCommandUpdater status(MatchStatus status);
    MatchCommandUpdater progress(Float progress);
    MatchCommandUpdater errorMessage(String errorMessage);
    MatchCommandUpdater rowsMatched(Integer rowsMatched);
    MatchCommandUpdater duration(int duration);
    MatchCommandUpdater dnbCommands();
    MatchCommandUpdater resultLocation(String location);
    MatchCommand commit();

}

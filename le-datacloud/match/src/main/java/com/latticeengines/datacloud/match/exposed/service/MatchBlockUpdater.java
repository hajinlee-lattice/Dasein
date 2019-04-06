package com.latticeengines.datacloud.match.exposed.service;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;

public interface MatchBlockUpdater {

    MatchBlockUpdater status(YarnApplicationState status);
    MatchBlockUpdater progress(Float progress);
    MatchBlockUpdater errorMessage(String errorMessage);
    MatchBlockUpdater matchedRows(int matchedRows);
    MatchBlockUpdater matchResults(Map<EntityMatchResult, Long> matchResultMap);
    MatchBlock commit();

}

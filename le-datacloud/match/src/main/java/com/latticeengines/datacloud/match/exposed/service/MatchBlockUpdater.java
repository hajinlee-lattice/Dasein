package com.latticeengines.datacloud.match.exposed.service;

import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;

public interface MatchBlockUpdater {
    
    MatchBlockUpdater status(YarnApplicationState status);
    MatchBlockUpdater progress(Float progress);
    MatchBlockUpdater errorMessage(String errorMessage);
    MatchBlockUpdater matchedRows(int matchedRows);
    MatchBlock commit();
    
}

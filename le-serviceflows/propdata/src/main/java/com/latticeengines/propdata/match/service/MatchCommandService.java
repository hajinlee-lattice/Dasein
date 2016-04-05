package com.latticeengines.propdata.match.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;

public interface MatchCommandService {

    MatchCommand start(MatchInput input, ApplicationId appId, String rootOperationUid);

    MatchCommandUpdater update(String rootOperationUid);

    MatchCommand getByRootOperationUid(String rootOperationUid);

}

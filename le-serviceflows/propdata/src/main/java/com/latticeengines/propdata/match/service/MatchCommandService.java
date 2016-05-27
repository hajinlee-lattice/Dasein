package com.latticeengines.propdata.match.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;

public interface MatchCommandService {

    MatchCommand start(MatchInput input, ApplicationId appId, String rootOperationUid);

    MatchBlock startBlock(MatchCommand matchCommand, ApplicationId appId, String blockOperationUid, Integer blockSize);

    MatchCommandUpdater update(String rootOperationUid);

    MatchCommand getByRootOperationUid(String rootOperationUid);

    MatchBlockUpdater updateBlock(String blockOperationUid);

    MatchBlock updateBlockByApplicationReport(String blockOperationUid, ApplicationReport report);

    Boolean blockIsRetriable(String blockOperationUid);

    MatchBlock retryBlock(String blockOperationUid, ApplicationId applicationId);

}

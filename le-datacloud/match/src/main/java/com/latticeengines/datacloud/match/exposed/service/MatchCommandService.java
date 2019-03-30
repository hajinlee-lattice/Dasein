package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public interface MatchCommandService {

    MatchCommand start(MatchInput input, ApplicationId appId, String rootOperationUid);

    MatchBlock startBlock(MatchCommand matchCommand, ApplicationId appId, String blockOperationUid, Integer blockSize);

    MatchCommandUpdater update(String rootOperationUid);

    MatchCommand getByRootOperationUid(String rootOperationUid);

    MatchBlockUpdater updateBlock(String blockOperationUid);

    MatchBlock updateBlockByApplicationReport(String blockOperationUid, ApplicationReport report);

    boolean blockIsRetriable(String blockOperationUid);

    MatchBlock retryBlock(String blockOperationUid, ApplicationId applicationId);

    List<MatchBlock> getBlocks(String rootOperationUid);

}

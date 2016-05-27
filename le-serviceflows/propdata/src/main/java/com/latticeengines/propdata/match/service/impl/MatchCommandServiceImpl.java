package com.latticeengines.propdata.match.service.impl;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.manage.MatchBlock;
import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.match.entitymgr.MatchBlockEntityMgr;
import com.latticeengines.propdata.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.propdata.match.service.MatchBlockUpdater;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.service.MatchCommandUpdater;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    private static Log log = LogFactory.getLog(MatchCommandServiceImpl.class);
    private static final Integer MAX_RETRIES = 2;

    @Autowired
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Autowired
    private MatchBlockEntityMgr matchBlockEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public MatchCommand start(MatchInput input, ApplicationId appId, String rootOperationUid) {
        MatchCommand command = new MatchCommand();
        command.setColumnSelection(columnSelectionString(input));
        command.setCustomer(input.getTenant().getId());

        command.setMatchStatus(MatchStatus.NEW);
        command.setRowsRequested(input.getNumRows());

        Date now = new Date();
        command.setCreateTime(now);
        command.setLatestStatusUpdate(now);

        command.setApplicationId(appId.toString());
        command.setProgress(0f);
        command.setRootOperationUid(rootOperationUid);

        return matchCommandEntityMgr.createCommand(command);
    }

    @Override
    public MatchBlock startBlock(MatchCommand matchCommand, ApplicationId appId, String blockOperationUid,
            Integer blockSize) {
        MatchBlock matchBlock = new MatchBlock();
        matchBlock.setMatchCommand(matchCommand);
        matchBlock.setApplicationId(appId.toString());
        matchBlock.setApplicationState(YarnApplicationState.NEW);
        matchBlock.setBlockOperationUid(blockOperationUid);
        matchBlock.setProgress(0f);
        matchBlock.setNumRows(blockSize);

        Date now = new Date();
        matchBlock.setCreateTime(now);
        matchBlock.setLatestStatusUpdate(now);
        return matchBlockEntityMgr.createBlock(matchBlock);
    }

    @Override
    public MatchCommandUpdaterImpl update(String rootOperationUid) {
        MatchCommand matchCommand = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
        if (matchCommand == null) {
            throw new RuntimeException("Cannot find a match command with root operation uid " + rootOperationUid);
        }
        return new MatchCommandUpdaterImpl(matchCommand);
    }

    @Override
    public MatchBlockUpdaterImpl updateBlock(String blockOperationUid) {
        MatchBlock matchBlock = matchBlockEntityMgr.findByBlockUid(blockOperationUid);
        if (matchBlock == null) {
            throw new RuntimeException("Cannot find a match block with block operation uid " + blockOperationUid);
        }
        return new MatchBlockUpdaterImpl(matchBlock);
    }

    @Override
    public Boolean blockIsRetriable(String blockOperationUid) {
        MatchBlock matchBlock = matchBlockEntityMgr.findByBlockUid(blockOperationUid);
        if (matchBlock == null) {
            throw new RuntimeException("Cannot find a match block with block operation uid " + blockOperationUid);
        }
        return matchBlock.getNumRetries() < MAX_RETRIES;
    }

    @Override
    public MatchBlock retryBlock(String blockOperationUid, ApplicationId applicationId) {
        return updateBlock(blockOperationUid).retry(applicationId).commit();
    }

    @Override
    public MatchBlock updateBlockByApplicationReport(String blockOperationUid, ApplicationReport report) {
        FinalApplicationStatus status = report.getFinalApplicationStatus();
        MatchBlockUpdaterImpl matchBlockUpdater = updateBlock(blockOperationUid)
                .status(report.getYarnApplicationState()).progress(report.getProgress());
        String rootOperationUid = matchBlockUpdater.getRootOperationUid();
        String blockErrorFile = hdfsPathBuilder.constructMatchBlockErrorFile(rootOperationUid, blockOperationUid)
                .toString();
        if (YarnUtils.TERMINAL_STATUS.contains(status)) {
            try {
                String blockError = HdfsUtils.getHdfsFileContents(yarnConfiguration, blockErrorFile);
                String errorMsg = blockError.split("\n")[0];
                matchBlockUpdater.errorMessage(errorMsg);
            } catch (Exception e) {
                log.error("Failed to read the error for matcher " + blockOperationUid + " in application "
                        + report.getApplicationId() + " : " + e.getMessage());
            }
        }
        return matchBlockUpdater.commit();
    }

    @Override
    public MatchCommand getByRootOperationUid(String rootOperationUid) {
        return matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
    }

    private static String columnSelectionString(MatchInput input) {
        String version;
        if (input.getPredefinedSelection() != null) {
            version = input.getPredefinedSelection().getName();
            if (StringUtils.isNotEmpty(input.getPredefinedVersion())) {
                version += "|" + input.getPredefinedVersion();
            }
        } else {
            version = JsonUtils.serialize(input.getCustomSelection());
        }
        return version;
    }

    public class MatchCommandUpdaterImpl implements MatchCommandUpdater {

        private MatchCommand matchCommand;

        MatchCommandUpdaterImpl(MatchCommand matchCommand) {
            this.matchCommand = matchCommand;
        }

        public MatchCommandUpdaterImpl status(MatchStatus status) {
            if (MatchStatus.FAILED.equals(status) || MatchStatus.ABORTED.equals(status)) {
                matchCommand.setStatusBeforeFailed(matchCommand.getMatchStatus());
            }
            matchCommand.setMatchStatus(status);
            return this;
        }

        public MatchCommandUpdaterImpl progress(Float progress) {
            matchCommand.setProgress(progress);
            return this;
        }

        public MatchCommandUpdaterImpl errorMessage(String errorMessage) {
            matchCommand.setErrorMessage(errorMessage.substring(0, Math.min(errorMessage.length(), 1000)));
            return this;
        }

        public MatchCommandUpdaterImpl rowsMatched(Integer rowsMatched) {
            matchCommand.setRowsMatched(rowsMatched);
            return this;
        }

        public MatchCommandUpdaterImpl resultLocation(String location) {
            matchCommand.setResultLocation(location);
            return this;
        }

        public MatchCommand commit() {
            matchCommand.setLatestStatusUpdate(new Date());
            return matchCommandEntityMgr.updateCommand(matchCommand);
        }

    }

    public class MatchBlockUpdaterImpl implements MatchBlockUpdater {

        private MatchBlock matchBlock;

        MatchBlockUpdaterImpl(MatchBlock matchBlock) {
            this.matchBlock = matchBlock;
        }

        public MatchBlockUpdaterImpl status(YarnApplicationState status) {
            if (YarnApplicationState.FAILED.equals(status) || YarnApplicationState.KILLED.equals(status)) {
                matchBlock.setStateBeforeFailed(matchBlock.getApplicationState());
            }
            matchBlock.setApplicationState(status);
            return this;
        }

        public MatchBlockUpdaterImpl progress(Float progress) {
            matchBlock.setProgress(progress);
            return this;
        }

        public MatchBlockUpdaterImpl errorMessage(String errorMessage) {
            matchBlock.setErrorMessage(errorMessage);
            return this;
        }

        public MatchBlockUpdaterImpl retry(ApplicationId appId) {
            matchBlock.setApplicationId(appId.toString());
            matchBlock.setApplicationState(YarnApplicationState.NEW);
            matchBlock.setProgress(0f);
            matchBlock.setNumRetries(matchBlock.getNumRetries() + 1);
            return this;
        }

        public MatchBlock commit() {
            matchBlock.setLatestStatusUpdate(new Date());
            return matchBlockEntityMgr.updateBlock(matchBlock);
        }

        String getRootOperationUid() {
            return matchBlock.getMatchCommand().getRootOperationUid();
        }

    }

}

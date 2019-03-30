package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.entitymgr.DnbMatchCommandEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MatchBlockEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MatchBlockUpdater;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandService;
import com.latticeengines.datacloud.match.exposed.service.MatchCommandUpdater;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.manage.DnBMatchCommand;
import com.latticeengines.domain.exposed.datacloud.manage.MatchBlock;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchStatus;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    private static Logger log = LoggerFactory.getLogger(MatchCommandServiceImpl.class);

    @Value("${datacloud.match.block.attempts.max}")
    private Integer maxBlockAttempts;

    @Inject
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Inject
    private MatchBlockEntityMgr matchBlockEntityMgr;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private YarnClient yarnClient;

    @Inject
    private DnbMatchCommandEntityMgr dnbMatchCommandEntityMgr;

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

        if (appId != null) {
            command.setApplicationId(appId.toString());
        }
        command.setProgress(0f);
        command.setRootOperationUid(rootOperationUid);

        command.setJobType(input.getRequestSource());
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
    public List<MatchBlock> getBlocks(String rootOperationUid) {
        return matchCommandEntityMgr.findBlocks(rootOperationUid);
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
    public boolean blockIsRetriable(String blockOperationUid) {
        MatchBlock matchBlock = matchBlockEntityMgr.findByBlockUid(blockOperationUid);
        if (matchBlock == null) {
            throw new RuntimeException("Cannot find a match block with block operation uid " + blockOperationUid);
        }
        // #attempt = #retries + 1: 1st attempt has #retries as 0
        return matchBlock.getNumRetries() + 1 < maxBlockAttempts;
    }

    @Override
    public MatchBlock retryBlock(String blockOperationUid, ApplicationId applicationId) {
        return updateBlock(blockOperationUid).retry(applicationId).commit();
    }

    @Override
    public MatchBlock updateBlockByApplicationReport(String blockOperationUid, ApplicationReport report) {
        FinalApplicationStatus status = report.getFinalApplicationStatus();
        MatchBlockUpdaterImpl matchBlockUpdater = updateBlock(blockOperationUid).status(
                report.getYarnApplicationState()).progress(report.getProgress());
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
        MatchCommand command = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);

        if (command == null) {
            return null;
        }

        if (MatchStatus.NEW.equals(command.getMatchStatus())
                || command.getLatestStatusUpdate() == null
                || (System.currentTimeMillis() - command.getLatestStatusUpdate().getTime()) > TimeUnit.MINUTES
                        .toMillis(20)) {
            String appIdStr = command.getApplicationId();
            if (StringUtils.isNotEmpty(appIdStr)) {
                FinalApplicationStatus status = getAppStatus(appIdStr);
                if (FinalApplicationStatus.FAILED.equals(status) || FinalApplicationStatus.KILLED.equals(status)) {
                    MatchStatus matchStatus = command.getMatchStatus();
                    switch (status) {
                    case FAILED:
                        matchStatus = MatchStatus.FAILED;
                        break;
                    case KILLED:
                        matchStatus = MatchStatus.ABORTED;
                    default:
                        break;
                    }
                    update(rootOperationUid).status(matchStatus).commit();
                    command = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
                }
            }
        }
        return command;
    }

    private FinalApplicationStatus getAppStatus(String appIdStr) {
        int retries = 0;
        long sleepTime = 500L;
        while (retries++ < 5) {
            try {
                ApplicationId applicationId = ConverterUtils.toApplicationId(appIdStr);
                ApplicationReport report = YarnUtils.getApplicationReport(yarnClient, applicationId);
                if (report == null) {
                    throw new IOException("Cannot find application report for ApplicationId " + applicationId);
                }
                return report.getFinalApplicationStatus();
            } catch (IOException e) {
                log.error("Failed to get application status for " + appIdStr + " retries=" + retries, e);
                try {
                    Thread.sleep(sleepTime);
                    sleepTime += sleepTime;
                } catch (InterruptedException e2) {
                    // ignore;
                }
            }
        }
        throw new RuntimeException("Failed to get final application status for " + appIdStr + " within 5 retries.");
    }

    private static String columnSelectionString(MatchInput input) {
        String version = "";
        if (input.getPredefinedSelection() != null) {
            version = input.getPredefinedSelection().getName();
            if (StringUtils.isNotEmpty(input.getPredefinedVersion())) {
                version += "|" + input.getPredefinedVersion();
            }
        } else if (input.getCustomSelection() != null) {
            version = input.getCustomSelection().getName();
            if (StringUtils.isNotEmpty(input.getCustomSelection().getVersion())) {
                version += "|" + input.getCustomSelection().getVersion();
            }
        }
        return version;
    }

    public class MatchCommandUpdaterImpl implements MatchCommandUpdater {

        private MatchCommand matchCommand;

        MatchCommandUpdaterImpl(MatchCommand matchCommand) {
            this.matchCommand = matchCommand;
        }

        @Override
        public MatchCommandUpdaterImpl status(MatchStatus status) {
            if (MatchStatus.FAILED.equals(status) || MatchStatus.ABORTED.equals(status)) {
                matchCommand.setStatusBeforeFailed(matchCommand.getMatchStatus());
            }
            matchCommand.setMatchStatus(status);
            return this;
        }

        @Override
        public MatchCommandUpdaterImpl progress(Float progress) {
            matchCommand.setProgress(progress);
            return this;
        }

        @Override
        public MatchCommandUpdaterImpl errorMessage(String errorMessage) {
            matchCommand.setErrorMessage(errorMessage.substring(0, Math.min(errorMessage.length(), 1000)));
            return this;
        }

        @Override
        public MatchCommandUpdaterImpl rowsMatched(Integer rowsMatched) {
            matchCommand.setRowsMatched(rowsMatched);
            return this;
        }

        @Override
        public MatchCommandUpdater rowsRequested(Integer rowsRequested) {
            matchCommand.setRowsRequested(rowsRequested);
            return this;
        }

        @Override
        public MatchCommandUpdaterImpl dnbCommands() {
            // populating the list of DnBMatchCommand into MatchCommand
            List<DnBMatchCommand> dnbMatchList = dnbMatchCommandEntityMgr.findAllByField("RootOperationUID",
                    matchCommand.getRootOperationUid());
            // computation from populated list
            int rowsMatchedByDnb = 0;
            int totalDnbDuration = 0;
            int rowsToDnb = 0;
            for (DnBMatchCommand command : dnbMatchList) {
                if (!command.getDnbCode().equals(DnBReturnCode.ABANDONED)) {
                    rowsToDnb += command.getSize();
                    rowsMatchedByDnb += command.getAcceptedRecords();
                    totalDnbDuration += command.getDuration();
                }
            }
            // rows to dnb
            matchCommand.setRowsToDnb(rowsToDnb);
            int dnbDurationAvg = 0;
            // To avoid null pointer exception when dnbMatchList.size() = 0
            if (dnbMatchList.size() != 0) {
                // To calculate average on request
                dnbDurationAvg = (totalDnbDuration) / dnbMatchList.size();
            }
            // rows matched by dnb
            matchCommand.setRowsMatchedByDnb(rowsMatchedByDnb);
            // average dnb duration
            matchCommand.setDnbDurationAvg(dnbDurationAvg);
            return this;
        }

        @Override
        public MatchCommandUpdaterImpl resultLocation(String location) {
            matchCommand.setResultLocation(location);
            return this;
        }

        @Override
        public MatchCommand commit() {
            matchCommand.setLatestStatusUpdate(new Date());
            synchronized (MatchCommandServiceImpl.class) {
                return matchCommandEntityMgr.updateCommand(matchCommand);
            }
        }

    }

    public class MatchBlockUpdaterImpl implements MatchBlockUpdater {

        private MatchBlock matchBlock;

        MatchBlockUpdaterImpl(MatchBlock matchBlock) {
            this.matchBlock = matchBlock;
        }

        @Override
        public MatchBlockUpdaterImpl status(YarnApplicationState status) {
            if (YarnApplicationState.FAILED.equals(status) || YarnApplicationState.KILLED.equals(status)) {
                matchBlock.setStateBeforeFailed(matchBlock.getApplicationState());
            }
            matchBlock.setApplicationState(status);
            return this;
        }

        @Override
        public MatchBlockUpdaterImpl progress(Float progress) {
            matchBlock.setProgress(progress);
            return this;
        }

        @Override
        public MatchBlockUpdaterImpl errorMessage(String errorMessage) {
            matchBlock.setErrorMessage(errorMessage);
            return this;
        }


        @Override
        public MatchBlockUpdater matchedRows(int matchedRows) {
            matchBlock.setMatchedRows(matchedRows);
            return this;
        }

        public MatchBlockUpdaterImpl retry(ApplicationId appId) {
            matchBlock.setApplicationId(appId.toString());
            matchBlock.setApplicationState(YarnApplicationState.NEW);
            matchBlock.setProgress(0f);
            matchBlock.setNumRetries(matchBlock.getNumRetries() + 1);
            return this;
        }

        @Override
        public MatchBlock commit() {
            matchBlock.setLatestStatusUpdate(new Date());
            synchronized (MatchCommandServiceImpl.class) {
                return matchBlockEntityMgr.updateBlock(matchBlock);
            }
        }

        String getRootOperationUid() {
            return matchBlock.getMatchCommand().getRootOperationUid();
        }

    }

}

package com.latticeengines.propdata.match.service.impl;

import java.util.Date;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.MatchCommand;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.match.entitymgr.MatchCommandEntityMgr;
import com.latticeengines.propdata.match.service.MatchCommandService;
import com.latticeengines.propdata.match.service.MatchCommandUpdater;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    @Autowired
    private MatchCommandEntityMgr matchCommandEntityMgr;

    @Override
    public MatchCommand start(MatchInput input, ApplicationId appId, String rootOperationUid) {
        MatchCommand command = new MatchCommand();
        command.setColumnSelection(input.getPredefinedSelection().getName());
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
    public MatchCommandUpdater update(String rootOperationUid) {
        MatchCommand matchCommand = matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
        if (matchCommand == null) {
            throw new RuntimeException("Cannot find a match command with root operation uid " + rootOperationUid);
        }
        return new MatchCommandUpdaterImpl(matchCommand);
    }

    @Override
    public MatchCommand getByRootOperationUid(String rootOperationUid) {
        return matchCommandEntityMgr.findByRootOperationUid(rootOperationUid);
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
            matchCommand.setErrorMessage(errorMessage);
            return this;
        }

        public MatchCommandUpdaterImpl rowsMatched(Integer rowsMatched) {
            matchCommand.setRowsMatched(rowsMatched);
            return this;
        }

        public MatchCommand commit() {
            matchCommand.setLatestStatusUpdate(new Date());
            return matchCommandEntityMgr.updateCommand(matchCommand);
        }

    }

}

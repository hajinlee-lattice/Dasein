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
        command.setNumRows(input.getNumRows());

        Date now = new Date();
        command.setCreateTime(now);
        command.setLatestStatusUpdate(now);

        command.setApplicationId(appId.toString());
        command.setProgress(0f);
        command.setRootOperationUid(rootOperationUid);

        return matchCommandEntityMgr.createCommand(command);
    }

}

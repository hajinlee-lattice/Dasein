package com.latticeengines.propdata.api.service.impl;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.Command;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.entitymanager.PropDataEntityMgr;
import com.latticeengines.propdata.api.service.MatchCommandService;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {
    @Autowired
    private PropDataEntityMgr entityMgr;

    @Override
    public Command createMatchCommand(CreateCommandRequest request) {
        return entityMgr.createCommand(
                request.getSourceTable(),
                request.getContractExternalID(),
                request.getDestTables());
    }

    @Override
    public Command findMatchCommandById(Long commandId) {
        return entityMgr.getCommand(commandId);
    }

    @Override
    public MatchCommandStatus getMatchCommandStatus(Long commandID) {
        return entityMgr.getMatchCommandStatus(commandID);
    }

    @Override
    public Collection<String> generatedResultTables(Long commandId) {
        return entityMgr.generatedResultTables(commandId);
    }

    @Override
    public boolean resultTablesAreReady(Long commandId) {
        return entityMgr.resultTablesAreReady(commandId);
    }
}

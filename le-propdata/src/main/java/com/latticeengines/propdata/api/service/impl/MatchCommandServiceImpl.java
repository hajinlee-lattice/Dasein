package com.latticeengines.propdata.api.service.impl;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.entitymanager.CommandEntityMgr;
import com.latticeengines.propdata.api.service.MatchCommandService;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {
    @Autowired
    private CommandEntityMgr entityMgr;

    @Override
    public Commands createMatchCommand(CreateCommandRequest request) {
        return entityMgr.createCommand(
                request.getSourceTable(),
                request.getContractExternalID(),
                request.getDestTables());
    }

    @Override
    public Commands findMatchCommandById(Long commandId) {
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

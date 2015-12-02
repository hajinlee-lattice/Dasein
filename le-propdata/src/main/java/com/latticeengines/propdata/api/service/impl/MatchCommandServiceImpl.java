package com.latticeengines.propdata.api.service.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.api.entitymanager.CommandEntityMgr;
import com.latticeengines.propdata.api.service.MatchCommandService;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    @Value("${propdata.matcher.available.clients}")
    private String availableClientsNames;

    @Value("${propdata.matcher.default.client}")
    private String defaultClient;

    private Set<MatchClient> availableClients;

    @PostConstruct
    private void parseAvailableClients() {
        availableClients = new HashSet<>();
        for (String clientName: availableClientsNames.split(",")) {
            availableClients.add(MatchClient.valueOf(clientName));
        }
    }

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

    @Override
    public MatchClient getBestMatchClient() {
        return MatchClient.valueOf(defaultClient);
    }

    @Override
    public MatchClient getMatchClientByName(String clientName) {
        if ("Default".equals(clientName)) { clientName = defaultClient; }
        MatchClient client = MatchClient.valueOf(clientName);
        if (!availableClients.contains(client)) {
            throw new LedpException(LedpCode.LEDP_25004, new String[]{clientName});
        }
        return client;
    }
}

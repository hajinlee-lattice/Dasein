package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.propdata.match.entitymanager.CommandEntityMgr;
import com.latticeengines.propdata.match.service.MatchCommandService;

@Component("matchCommandService")
public class MatchCommandServiceImpl implements MatchCommandService {

    @Value("${propdata.matcher.available.clients}")
    private String availableClientsNames;

    @Value("${propdata.matcher.default.client}")
    private String defaultClient;

    private List<MatchClient> availableClients;
    private static int roundRobinPos = 0;
    private static final int BLOCK_SIZE = 1500;
    private static final int BIG_MATCH_THRESHOLD = 500 * 1000;

    @PostConstruct
    private void parseAvailableClients() {
        availableClients = new ArrayList<>();
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
    public MatchClientDocument getBestMatchClient(int numRows) {
        return new MatchClientDocument(roundRobinLoadBalancing(numRows));
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

    private MatchClient roundRobinLoadBalancing(int numRows) {
        if (availableClients.size() == 1) { return MatchClient.valueOf(defaultClient); }
        if (numRows <= BLOCK_SIZE && availableClients.contains(MatchClient.PD126)) {
            return MatchClient.PD126;
        }
        if (numRows >= BIG_MATCH_THRESHOLD && availableClients.contains(MatchClient.PD144)) {
            return MatchClient.PD144;
        }
        roundRobinPos = (roundRobinPos + 1) % availableClients.size();
        return availableClients.get(roundRobinPos);
    }

}

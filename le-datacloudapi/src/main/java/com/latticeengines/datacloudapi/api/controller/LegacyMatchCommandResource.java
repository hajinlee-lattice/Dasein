package com.latticeengines.datacloudapi.api.controller;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.datacloud.MatchClient;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "matchcommands", description = "REST resource for match commands")
@RestController
@RequestMapping("/matchcommands")
public class LegacyMatchCommandResource {

    @Value("${datacloud.match.matcher.available.clients}")
    private String availableClientsNames;

    @Value("${datacloud.match.matcher.default.client}")
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

    @RequestMapping(value = "/bestclient", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Return the best matcher client to use. " +
            "Requires a query parameter \"rows\", which is the number of records to be matched.")
    public MatchClientDocument getBestMatchClient(@RequestParam(value = "rows", required = true) int numRows) {
        return new MatchClientDocument(roundRobinLoadBalancing(numRows));
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

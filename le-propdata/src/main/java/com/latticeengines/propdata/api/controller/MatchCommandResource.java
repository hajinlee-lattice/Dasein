package com.latticeengines.propdata.api.controller;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
import com.latticeengines.domain.exposed.propdata.MatchStatusResponse;
import com.latticeengines.propdata.api.datasource.MatchClientContextHolder;
import com.latticeengines.propdata.api.service.MatchCommandService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Set;

@Api(value = "matchcommands", description = "REST resource for match commands")
@RestController
@RequestMapping("/matchcommands")
public class MatchCommandResource {

    @Autowired
    private MatchCommandService matchCommandService;

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

    @RequestMapping(value = "/{commandID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the status of a match command on the specific client. " +
            "URL parameter matchClient can be PD126, PD127, PD128, PD131, or PD130. " +
            "PD130 should be used only in QA.")
    public MatchStatusResponse getMatchStatus(@PathVariable Long commandID,
                                             @RequestParam(value="matchClient", required = false, defaultValue = "Default") String clientName) {
        configClientFromRequest(clientName);
        MatchCommandStatus status = matchCommandService.getMatchCommandStatus(commandID);
        return new MatchStatusResponse(status);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a match command on the specific client. " +
            "URL parameter matchClient can be PD126, PD127, PD128, PD131, or PD130. " +
            "PD130 should be used only in QA.")
    public Commands createMatchCommand(@RequestBody CreateCommandRequest request,
                                      @RequestParam(value="matchClient", required=false, defaultValue = "Default") String clientName) {
        configClientFromRequest(clientName);
        return matchCommandService.createMatchCommand(request);
    }

    private void configClientFromRequest(String clientName) {
        if ("Default".equals(clientName)) { clientName = defaultClient; }
        MatchClient client = MatchClient.valueOf(clientName);
        if (!availableClients.contains(client)) {
            throw new LedpException(LedpCode.LEDP_25004, new String[]{clientName});
        }
        MatchClientContextHolder.setMatchClient(client);
    }

}

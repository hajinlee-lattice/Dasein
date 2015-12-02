package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
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

@Api(value = "matchcommands", description = "REST resource for match commands")
@RestController
@RequestMapping("/matchcommands")
public class MatchCommandResource {

    @Autowired
    private MatchCommandService matchCommandService;

    @RequestMapping(value = "/{commandID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the status of a match command on the specific client. " +
            "URL parameter matchClient can be PD126, PD127, PD128, PD131, or PD130. " +
            "PD130 should be used only in QA.")
    public MatchStatusResponse getMatchStatus(@PathVariable Long commandID,
                                             @RequestParam(value="matchClient", required = false, defaultValue = "Default") String clientName) {
        MatchClientContextHolder.setMatchClient(matchCommandService.getMatchClientByName(clientName));
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
        MatchClientContextHolder.setMatchClient(matchCommandService.getMatchClientByName(clientName));
        return matchCommandService.createMatchCommand(request);
    }

    @RequestMapping(value = "/bestclient", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Return the best matcher client to use.")
    public MatchClient getBestMatchClient() { return matchCommandService.getBestMatchClient(); }

}

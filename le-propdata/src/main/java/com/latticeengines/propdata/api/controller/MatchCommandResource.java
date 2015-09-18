package com.latticeengines.propdata.api.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.latticeengines.domain.exposed.propdata.Commands;
import com.latticeengines.domain.exposed.propdata.CreateCommandRequest;
import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchCommandStatus;
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
    @ApiOperation(value = "Get status of match command")
    public MatchCommandStatus getMatchStatus(@PathVariable Long commandID,
                                             @RequestParam(value="matchClient") String clientName) {
        MatchClient client = MatchClient.valueOf(clientName);
        MatchClientContextHolder.setMatchClient(client);
        return matchCommandService.getMatchCommandStatus(commandID);
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status of match command")
    public Commands createMatchCommand(@RequestBody CreateCommandRequest request,
                                      @RequestParam(value="matchClient", required=false) String clientName) {
        MatchClient client = MatchClient.valueOf(clientName);
        MatchClientContextHolder.setMatchClient(client);
        return matchCommandService.createMatchCommand(request);
    }
}

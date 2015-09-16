package com.latticeengines.propdata.api.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;

@Api(value = "matchcommand", description = "REST resource for match commands")
@RestController
@RequestMapping("/matchcommands")
public class MatchCommandResource {

//    @RequestMapping(value = "/{commandID}", method = RequestMethod.GET, headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Get status of match command")
//    public ResponseCommandStatus getMatchStatus(@PathVariable String commandID
//                                        ,@RequestParam(value="matchClient", required=false) String matchClient) {
//
//        String status =
//                matchCommandService.getMatchCommandStatus(commandID,matchClient);
//
//        return new ResponseCommandStatus(true, null, status);
//    }
//
//    @RequestMapping(value = "", method = RequestMethod.PUT, headers = "Accept=application/json")
//    @ResponseBody
//    @ApiOperation(value = "Create a new command")
//    public ResponseID createMatchCommand(@RequestParam(value = "sourceTable", required = true) String sourceTable
//                                    ,@RequestParam(value = "destTables", required = true) String destTables
//                                    ,@RequestParam(value = "contractExternalID", required = true) String contractExternalID
//                                    ,@RequestParam(value = "matchClient", required = false) String matchClient) {
//        Long commandID = matchCommandService
//                .createMatchCommand(sourceTable,destTables,contractExternalID,matchClient);
//
//        return new ResponseID(true, null, commandID);
//    }
}

package com.latticeengines.pls.controller;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scoring", description = "REST resource for interacting with scoring workflows")
@RestController
@RequestMapping("/scoring")
@PreAuthorize("hasRole('Edit_PLS_Models')")
public class ScoringResource {

    @RequestMapping(value = "/{filename}", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke a scoring run with the provided uploaded file")
    public ResponseDocument<String> score(@RequestParam("filename") String filename) {
        // TODO
        return new ResponseDocument<>("LosLobosKickYourBallsIntoOuterSpace!");
    }
}

package com.latticeengines.dante.controller;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.network.exposed.dante.DanteTalkingPointInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Talking Points CRUD operationsdas")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource implements DanteTalkingPointInterface {
    private static final Logger log = Logger.getLogger(TalkingPointResource.class);

    @Autowired
    TalkingPointService talkingPointService;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> createOrUpdate(@RequestBody DanteTalkingPoint talkingPoint) {
        talkingPointService.createOrUpdate(talkingPoint);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{externalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('View_PLS_Plays')")
    public ResponseDocument<DanteTalkingPoint> findByExternalID(@PathVariable String externalID) {
        return ResponseDocument.successResponse(talkingPointService.findByExternalID(externalID));
    }

    @RequestMapping(value = "/play/{playExternalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('View_PLS_Plays')")
    public ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(@PathVariable String playExternalID) {
        return ResponseDocument.successResponse(talkingPointService.findAllByPlayID(playExternalID));
    }

    @RequestMapping(value = "/{talkingPointExternalID}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> delete(@PathVariable String talkingPointExternalID) {
        DanteTalkingPoint talkingPoint = talkingPointService.findByExternalID(talkingPointExternalID);
        talkingPointService.delete(talkingPoint);
        return SimpleBooleanResponse.successResponse();
    }
}

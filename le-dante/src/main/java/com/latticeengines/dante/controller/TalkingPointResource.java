package com.latticeengines.dante.controller;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
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
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.network.exposed.dante.DanteTalkingPointInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Talking Points CRUD operations")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource implements DanteTalkingPointInterface {

    @Autowired
    TalkingPointService talkingPointService;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> createOrUpdate(@RequestBody DanteTalkingPoint talkingPoint) {
        try {
            talkingPointService.createOrUpdate(talkingPoint);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils.getFullStackTrace(e)));
        }
    }

    @RequestMapping(value = "/{externalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<DanteTalkingPoint> findByExternalID(@PathVariable String externalID) {
        try {
            return ResponseDocument.successResponse(talkingPointService.findByExternalID(externalID));
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/play/{playExternalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(@PathVariable String playID) {
        try {
            return ResponseDocument.successResponse(talkingPointService.findAllByPlayID(playID));
        } catch (LedpException e) {
            return ResponseDocument.failedResponse(e);
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/{talkingPointExternalID}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> delete(@PathVariable String externalID) {
        try {
            DanteTalkingPoint talkingPoint = talkingPointService.findByExternalID(externalID);
            talkingPointService.delete(talkingPoint);
            return SimpleBooleanResponse.successResponse();
        } catch (LedpException e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(e.getMessage()));
        } catch (Exception e) {
            return SimpleBooleanResponse.failedResponse(Collections.singletonList(ExceptionUtils.getFullStackTrace(e)));
        }
    }

}

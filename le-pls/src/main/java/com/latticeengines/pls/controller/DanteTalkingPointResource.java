package com.latticeengines.pls.controller;

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

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.proxy.exposed.dante.DanteTalkingPointProxy;
import com.wordnik.swagger.annotations.Api;

import io.swagger.annotations.ApiOperation;

@Api(value = "dantetalkingpoint", description = "REST resource for segments")
@RestController
@RequestMapping("/dantetalkingpoint")
public class DanteTalkingPointResource {

    private static final Logger log = Logger.getLogger(DanteTalkingPointResource.class);

    @Autowired
    DanteTalkingPointProxy danteTalkingPointProxy;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> createOrUpdate(@RequestBody DanteTalkingPoint talkingPoint) {
        return danteTalkingPointProxy.createOrUpdate(talkingPoint);
    }

    @RequestMapping(value = "/{externalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<DanteTalkingPoint> findByExternalID(@PathVariable String externalID) {
        return danteTalkingPointProxy.findByExternalID(externalID);
    }

    @RequestMapping(value = "/play/{playExternalID}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<List<DanteTalkingPoint>> findAllByPlayID(@PathVariable String playExternalID) {
        return danteTalkingPointProxy.findAllByPlayID(playExternalID);
    }

    @RequestMapping(value = "/{talkingPointExternalID}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Dante Talking Point ")
    @PreAuthorize("hasRole('Edit_PLS_Plays')")
    public ResponseDocument<?> delete(@PathVariable String talkingPointExternalID) {
        return danteTalkingPointProxy.delete(talkingPointExternalID);
    }
}

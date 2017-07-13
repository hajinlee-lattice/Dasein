package com.latticeengines.dante.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.pls.TalkingPoint;
import com.latticeengines.network.exposed.dante.TalkingPointInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "dante", description = "REST resource for Dante Talking Points CRUD operationsdas")
@RestController
@RequestMapping("/talkingpoints")
public class TalkingPointResource implements TalkingPointInterface {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointResource.class);

    @Autowired
    TalkingPointService talkingPointService;

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseBody
    @ResponseStatus(HttpStatus.CREATED)
    @ApiOperation(value = "Create a Talking Point ")
    public ResponseDocument<?> createOrUpdate(@RequestBody List<TalkingPoint> talkingPoints) {
        talkingPointService.createOrUpdate(talkingPoints);
        return SimpleBooleanResponse.successResponse();
    }

    @RequestMapping(value = "/{name}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get a Talking Point ")
    public ResponseDocument<TalkingPoint> findByName(@PathVariable String name) {
        return ResponseDocument.successResponse(talkingPointService.findByName(name));
    }

    @RequestMapping(value = "/play/{playId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "find all Talking Points for the given play")
    public ResponseDocument<List<TalkingPoint>> findAllByPlayID(@PathVariable Long playId) {
        return ResponseDocument.successResponse(talkingPointService.findAllByPlayId(playId));
    }

    @RequestMapping(value = "/publish", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Publish given play's Talking Points to dante")
    public ResponseDocument<?> publish(@RequestParam("playId") Long playId) {
        try {
            talkingPointService.publish(playId);
            return SimpleBooleanResponse.successResponse();
        } catch (Exception e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @RequestMapping(value = "/previewresources", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "get the server url and oAuth token to preview Dante Talking Point")
    public ResponseDocument<DantePreviewResources> getPreviewResources(
            @RequestParam("customerSpace") String customerSpace) {
        return ResponseDocument.successResponse(talkingPointService.getPreviewResources(customerSpace));
    }

    @RequestMapping(value = "/{talkingPointExternalID}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a Talking Point ")
    public ResponseDocument<?> delete(@PathVariable String talkingPointName) {
        TalkingPoint talkingPoint = talkingPointService.findByName(talkingPointName);
        talkingPointService.delete(talkingPoint);
        return SimpleBooleanResponse.successResponse();
    }
}
